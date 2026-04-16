use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::ToDFSchema;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::expr::BinaryExpr as LogicalBinaryExpr;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::equivalence::EquivalenceProperties;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ColumnarValue, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use object_store::ObjectStore;
#[cfg(test)]
use object_store::local::LocalFileSystem;
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::ops::Range;
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs::group::Group;
use zarrs::storage::AsyncReadableListableStorageTraits;
use zarrs_icechunk::AsyncIcechunkStore;

use crate::error::{ZarrDataFusionError, ZarrDataFusionResult};
use crate::schema::group_arrays_schema_async;
use geo_index::rtree::{RTreeIndex, util::f64_box_to_f32};

/// Maps chunk indices to row indices within those chunks
type ChunkRowMap = HashMap<Vec<u64>, Vec<u64>>;

/// Result from spatial index query: (chunk_to_rows map, handled_filter_index)
type SpatialIndexResult = (ChunkRowMap, usize);

pub fn register_spatial_functions(ctx: &SessionContext) -> Result<()> {
    geodatafusion::register(ctx);
    Ok(())
}

#[derive(Debug)]
pub struct ZarrTableProvider {
    schema: SchemaRef,
    zarr_backend: ZarrBackend,
    group_path: String,
    /// Spatial R-tree indexes keyed by array name (e.g., "bbox" -> R-tree bytes)
    rtree_indexes: HashMap<String, Vec<u8>>,
}

impl ZarrTableProvider {
    pub async fn new_icechunk(
        icechunk_session: icechunk::session::Session,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let store = Arc::new(AsyncIcechunkStore::new(icechunk_session));
        let zarr_backend = IcechunkZarrBackend {
            store: store.clone(),
            handle: Handle::current(),
        };
        let schema = zarr_backend.infer_group_schema(group_path).await?;
        let rtree_indexes = Self::load_spatial_indexes(store.clone()).await;

        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
            group_path: group_path.to_string(),
            rtree_indexes,
        })
    }

    pub async fn new_object_store<T: ObjectStore>(
        store: T,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = AsyncZarrBackend::new_object_store(store);
        let rtree_indexes = Self::load_spatial_indexes(zarr_backend.store.clone()).await;

        let schema = zarr_backend.infer_group_schema(group_path).await?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
            group_path: group_path.to_string(),
            rtree_indexes,
        })
    }

    /// Load all spatial R-tree indexes from the /indexes group
    ///
    /// Returns a HashMap of array name -> R-tree bytes
    async fn load_spatial_indexes(
        store: Arc<dyn AsyncReadableListableStorageTraits>,
    ) -> HashMap<String, Vec<u8>> {
        let mut indexes = HashMap::new();

        // Try to open the /indexes group
        let indexes_group = match Group::async_open(store.clone(), "/indexes").await {
            Ok(group) => group,
            Err(_) => return indexes, // No indexes group exists
        };

        // List all arrays in the /indexes group
        let child_arrays = match indexes_group.async_child_arrays().await {
            Ok(arrays) => arrays,
            Err(_) => return indexes,
        };

        // Load each index array
        for array in child_arrays {
            let array_name = array.path().as_str().rsplit('/').next().unwrap_or("");
            if array_name.is_empty() {
                continue;
            }
            let index_path = format!("/indexes/{}", array_name);
            match Self::load_index(store.clone(), &index_path).await {
                Ok(index_bytes) => {
                    indexes.insert(array_name.to_string(), index_bytes);
                }
                Err(_) => {
                    // Failed to load index, skip it
                }
            }
        }

        indexes
    }

    async fn load_index(
        store: Arc<dyn AsyncReadableListableStorageTraits>,
        index_path: &str,
    ) -> ZarrDataFusionResult<Vec<u8>> {
        let index_array = Array::async_open(store, index_path).await?;
        let array_shape = index_array.shape();
        let rtree_bytes: Vec<u8> = index_array
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(
                array_shape.to_vec(),
            ))
            .await?;
        Ok(rtree_bytes)
    }
}

#[async_trait]
impl TableProvider for ZarrTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                // Check if this filter can be handled by spatial index
                if let Some((array_name, _)) = extract_st_intersects_bbox(filter)
                    && self.rtree_indexes.contains_key(&array_name)
                {
                    // We have a spatial index - we can handle this exactly
                    return TableProviderFilterPushDown::Exact;
                }
                // Otherwise inexact
                TableProviderFilterPushDown::Inexact
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema: SchemaRef = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };
        Ok(Arc::new(ZarrExec::new(
            self.zarr_backend.clone(),
            self.schema.clone(),
            projected_schema,
            filters.to_vec(),
            self.group_path.clone(),
            self.rtree_indexes.clone(),
        )))
    }
}

#[derive(Clone)]
struct IcechunkZarrBackend {
    store: Arc<dyn AsyncReadableListableStorageTraits>,
    handle: Handle,
}

impl IcechunkZarrBackend {
    async fn infer_group_schema(&self, group_path: &str) -> ZarrDataFusionResult<SchemaRef> {
        let group = Group::async_open(self.store.clone(), group_path).await?;
        group_arrays_schema_async(&group).await
    }
}

#[derive(Clone)]
struct AsyncZarrBackend {
    store: Arc<dyn AsyncReadableListableStorageTraits>,
    handle: Handle,
}

impl AsyncZarrBackend {
    fn new_object_store<T: ObjectStore>(store: T) -> Self {
        AsyncZarrBackend {
            store: Arc::new(zarrs_object_store::AsyncObjectStore::new(store)),
            handle: Handle::current(),
        }
    }

    async fn infer_group_schema(&self, group_path: &str) -> ZarrDataFusionResult<SchemaRef> {
        let group = Group::async_open(self.store.clone(), group_path).await?;
        group_arrays_schema_async(&group).await
    }
}

#[derive(Clone)]
enum ZarrBackend {
    Async(AsyncZarrBackend),
    Icechunk(IcechunkZarrBackend),
}

impl Debug for ZarrBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZarrBackend::Async(_) => write!(f, "ZarrBackend::Async"),
            ZarrBackend::Icechunk(_) => write!(f, "ZarrBackend::Icechunk"),
        }
    }
}

impl From<AsyncZarrBackend> for ZarrBackend {
    fn from(b: AsyncZarrBackend) -> Self {
        ZarrBackend::Async(b)
    }
}

impl From<IcechunkZarrBackend> for ZarrBackend {
    fn from(b: IcechunkZarrBackend) -> Self {
        ZarrBackend::Icechunk(b)
    }
}

#[derive(Debug)]
struct ZarrExec {
    zarr_backend: ZarrBackend,
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    group_path: String,
    indexes: HashMap<String, Vec<u8>>,
    properties: PlanProperties,
}

impl ZarrExec {
    fn new(
        zarr_backend: ZarrBackend,
        table_schema: SchemaRef,
        projected_schema: SchemaRef,
        filters: Vec<Expr>,
        group_path: String,
        indexes: HashMap<String, Vec<u8>>,
    ) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            zarr_backend,
            table_schema,
            projected_schema,
            filters,
            group_path,
            indexes,
            properties,
        }
    }

    fn filter_column_names(filters: &[Expr]) -> HashSet<String> {
        let mut cols = HashSet::new();
        for expr in filters {
            collect_columns_from_expr(expr, &mut cols);
        }
        cols
    }
}

impl ExecutionPlan for ZarrExec {
    fn name(&self) -> &str {
        "ZarrExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }
    fn properties(&self) -> &PlanProperties {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let backend = self.zarr_backend.clone();
        let table_schema = self.table_schema.clone();
        let projected_schema = self.projected_schema.clone();
        let filters = self.filters.clone();
        let filter_col_names = Self::filter_column_names(&filters);
        let group_path = self.group_path.clone();
        let indexes = self.indexes.clone();

        let (store, handle) = match &backend {
            ZarrBackend::Async(b) => (b.store.clone(), b.handle.clone()),
            ZarrBackend::Icechunk(b) => (b.store.clone(), b.handle.clone()),
        };

        let stream = RecordBatchStreamAdapter::new(
            self.projected_schema.clone(),
            futures::stream::once(async move {
                handle
                    .spawn(scan_chunks_async(
                        store,
                        group_path,
                        table_schema,
                        projected_schema,
                        filters,
                        filter_col_names,
                        indexes,
                    ))
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                    .map_err(|e: ZarrDataFusionError| datafusion::error::DataFusionError::from(e))
            })
            .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
            .try_flatten(),
        );
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for ZarrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ZarrExec: group={}, schema={:?}, filters={:?}",
            self.group_path, self.projected_schema, self.filters
        )
    }
}

/// Extract bounding box from ST_Intersects filter and query R-tree index
///
/// Returns (chunk_map, filter_index) where:
/// - chunk_map: HashMap of chunk indices -> row indices within that chunk
/// - filter_index: Index of the filter that was handled by the spatial index (to skip evaluation)
fn st_intersects_query_index(
    filters: &[Expr],
    indexes: &HashMap<String, Vec<u8>>,
    arrays: &HashMap<String, Array<Arc<dyn AsyncReadableListableStorageTraits>>>,
    chunk_grid_shape: &[u64],
) -> Option<SpatialIndexResult> {
    use geo_index::rtree::RTreeRef;

    // Look for ST_Intersects(column, ST_GeomFromText('POLYGON(...)'))
    for (filter_idx, filter) in filters.iter().enumerate() {
        if let Some((array_name, bbox)) = extract_st_intersects_bbox(filter) {
            // Check if we have an R-tree index for this array
            if let Some(index_bytes) = indexes.get(&array_name) {
                // Query the R-tree
                if let Ok(rtree) = RTreeRef::<f32>::try_new(index_bytes) {
                    let (minx, miny, maxx, maxy) = bbox;
                    let matching_indices = rtree.search(minx, miny, maxx, maxy);

                    // Group row indices by chunk
                    if let Some(array) = arrays.get(&array_name) {
                        let chunk_map =
                            group_row_indices_by_chunk(&matching_indices, array, chunk_grid_shape);
                        return Some((chunk_map, filter_idx));
                    }
                }
            }
        }
    }

    None // No spatial filter found or index unavailable
}

/// Extract (array_name, (minx, miny, maxx, maxy)) from ST_Intersects filter
fn extract_st_intersects_bbox(expr: &Expr) -> Option<(String, (f32, f32, f32, f32))> {
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::scalar::ScalarValue;

    // Match ST_Intersects(column, geometry_literal)
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return None;
    };

    if func.name() != "st_intersects" || args.len() != 2 {
        return None;
    }

    // First arg should be column reference
    let Expr::Column(col) = &args[0] else {
        return None;
    };
    let array_name = col.name.clone();

    // Second arg could be either:
    // 1. ST_GeomFromText('POLYGON(...)')
    // 2. Literal GeoArrow geometry (Union)
    match &args[1] {
        // Case 1: ST_GeomFromText function call
        Expr::ScalarFunction(ScalarFunction {
            func: geom_func,
            args: geom_args,
        }) if geom_func.name() == "st_geomfromtext" && !geom_args.is_empty() => {
            let Expr::Literal(ScalarValue::Utf8(Some(wkt)), _) = &geom_args[0] else {
                return None;
            };
            parse_polygon_bbox(wkt).map(|bbox| (array_name, bbox))
        }

        // Case 2: GeoArrow geometry literal (Union type)
        Expr::Literal(literal_value, _) => {
            extract_bbox_from_geoarrow_literal(literal_value).map(|bbox| (array_name, bbox))
        }

        _ => None,
    }
}

/// Extract bounding box from coordinate arrays
fn compute_bbox_from_coords(
    x_arr: &arrow::array::PrimitiveArray<arrow::datatypes::Float64Type>,
    y_arr: &arrow::array::PrimitiveArray<arrow::datatypes::Float64Type>,
) -> Option<(f32, f32, f32, f32)> {
    let mut minx = f64::MAX;
    let mut miny = f64::MAX;
    let mut maxx = f64::MIN;
    let mut maxy = f64::MIN;

    for i in 0..x_arr.len() {
        let x = x_arr.value(i);
        let y = y_arr.value(i);
        minx = minx.min(x);
        maxx = maxx.max(x);
        miny = miny.min(y);
        maxy = maxy.max(y);
    }

    if minx != f64::MAX {
        Some(f64_box_to_f32(minx, miny, maxx, maxy))
    } else {
        None
    }
}

/// Extract bounding box from a GeoArrow geometry literal
fn extract_bbox_from_geoarrow_literal(
    value: &datafusion::scalar::ScalarValue,
) -> Option<(f32, f32, f32, f32)> {
    use arrow::array::{Array, AsArray};
    use datafusion::scalar::ScalarValue;

    // GeoArrow geometries are stored as Union scalars
    // Structure: Union -> List (rings) -> List (vertices) -> Struct {x, y}
    let ScalarValue::Union(fields_opt, _type_ids, _metadata) = value else {
        return None;
    };

    let (_field_id, scalar_val) = fields_opt.as_ref()?;

    let ScalarValue::List(list_arr) = scalar_val.as_ref() else {
        return None;
    };

    if list_arr.is_empty() {
        return None;
    }

    let rings_array = list_arr.value(0);
    let vertices_list = rings_array.as_list_opt::<i32>()?;

    if vertices_list.is_empty() {
        return None;
    }

    let vertices = vertices_list.value(0);
    let struct_array = vertices.as_struct_opt()?;

    let x_col = struct_array.column_by_name("x")?;
    let y_col = struct_array.column_by_name("y")?;

    let x_arr = x_col.as_primitive_opt::<arrow::datatypes::Float64Type>()?;
    let y_arr = y_col.as_primitive_opt::<arrow::datatypes::Float64Type>()?;

    compute_bbox_from_coords(x_arr, y_arr)
}

/// Parse bounding box from WKT POLYGON string
///
/// Example: "POLYGON((0 -7, 0 7, 5 7, 5 -7, 0 -7))" -> (0.0, -7.0, 5.0, 7.0)
fn parse_polygon_bbox(wkt: &str) -> Option<(f32, f32, f32, f32)> {
    // Simple parser for POLYGON((x1 y1, x2 y2, ...))
    let coords_str = wkt.strip_prefix("POLYGON((")?.strip_suffix("))")?;

    let mut minx = f64::MAX;
    let mut miny = f64::MAX;
    let mut maxx = f64::MIN;
    let mut maxy = f64::MIN;

    for point in coords_str.split(',') {
        let coords: Vec<&str> = point.split_whitespace().collect();
        if coords.len() >= 2
            && let (Ok(x), Ok(y)) = (coords[0].parse::<f64>(), coords[1].parse::<f64>())
        {
            minx = minx.min(x);
            maxx = maxx.max(x);
            miny = miny.min(y);
            maxy = maxy.max(y);
        }
    }

    if minx != f64::MAX {
        Some(f64_box_to_f32(minx, miny, maxx, maxy))
    } else {
        None
    }
}

/// Group row indices by which chunk they belong to
///
/// Returns a HashMap where:
/// - Key: chunk indices (e.g., [0], [1], [2])
/// - Value: local row indices within that chunk (e.g., [123, 456, 789])
fn group_row_indices_by_chunk(
    row_indices: &[u32],
    array: &Array<Arc<dyn AsyncReadableListableStorageTraits>>,
    _chunk_grid_shape: &[u64],
) -> ChunkRowMap {
    let mut chunk_to_rows: ChunkRowMap = HashMap::new();

    // Get the chunk shape for the first chunk (assuming regular grid)
    let chunk_shape = match array.chunk_shape(&[0]) {
        Ok(shape) => shape,
        Err(_e) => {
            return chunk_to_rows; // Return empty if can't get chunk shape
        }
    };

    for &row_idx in row_indices {
        let row_idx = row_idx as u64;

        // For 1D array, calculate chunk index and local index within chunk
        if chunk_shape.len() == 1 {
            let chunk_size = chunk_shape[0];
            let chunk_idx = vec![row_idx / chunk_size];
            let local_idx = row_idx % chunk_size;

            chunk_to_rows.entry(chunk_idx).or_default().push(local_idx);
        } else {
            // For multi-dimensional, would need to calculate properly
            // For now, assume 1D
            let chunk_size = chunk_shape[0];
            let chunk_idx = vec![row_idx / chunk_size];
            let local_idx = row_idx % chunk_size;

            chunk_to_rows.entry(chunk_idx).or_default().push(local_idx);
        }
    }

    chunk_to_rows
}

async fn scan_chunks_async(
    store: Arc<dyn AsyncReadableListableStorageTraits>,
    group: String,
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    filter_col_names: HashSet<String>,
    rtree_indexes: HashMap<String, Vec<u8>>,
) -> ZarrDataFusionResult<Vec<RecordBatch>> {
    let all_cols: HashSet<String> = filter_col_names
        .iter()
        .chain(projected_schema.fields().iter().map(|f| f.name()))
        .cloned()
        .collect();

    let mut arrays: HashMap<String, Array<Arc<dyn AsyncReadableListableStorageTraits>>> =
        HashMap::new();
    for col_name in &all_cols {
        let path = format!("{group}/{col_name}");
        let array = Array::async_open(Arc::new(store.clone()), &path).await?;
        arrays.insert(col_name.clone(), array);
    }
    let chunk_grid_shape = arrays
        .values()
        .next()
        .map(|a| a.chunk_grid_shape())
        .ok_or(ZarrDataFusionError::Custom("No arrays to scan".into()))?;

    // Try to use spatial index to filter chunks
    let spatial_index_result =
        st_intersects_query_index(&filters, &rtree_indexes, &arrays, chunk_grid_shape);

    // If spatial index was used, remove that filter from evaluation (skip WKB decode!)
    let (filters_to_evaluate, filter_col_names_to_load) =
        if let Some((_, handled_filter_idx)) = &spatial_index_result {
            let mut remaining_filters = Vec::new();

            for (idx, filter) in filters.iter().enumerate() {
                if idx != *handled_filter_idx {
                    remaining_filters.push(filter.clone());
                }
            }

            // Recalculate which columns we need for the remaining filters
            let mut remaining_cols = HashSet::new();
            for filter in &remaining_filters {
                collect_columns_from_expr(filter, &mut remaining_cols);
            }

            (remaining_filters, remaining_cols)
        } else {
            (filters.clone(), filter_col_names.clone())
        };

    // Determine which chunks to scan and optionally which rows within each chunk
    let chunk_tasks: Vec<(Vec<u64>, Option<Vec<u64>>)> =
        if let Some((chunk_to_rows, _)) = spatial_index_result {
            // Use filtered chunks from spatial index with specific row indices
            chunk_to_rows
                .into_iter()
                .map(|(chunk_idx, row_indices)| (chunk_idx, Some(row_indices)))
                .collect()
        } else {
            // No spatial filter or index - scan all chunks, all rows
            let ranges: Vec<Range<u64>> = chunk_grid_shape.iter().map(|&n| 0..n).collect();
            ArraySubset::new_with_ranges(&ranges)
                .indices()
                .into_iter()
                .map(|chunk_idx| (chunk_idx, None))
                .collect()
        };

    // Wrap arrays in Arc so they can be shared across spawned tasks.
    let arrays = Arc::new(arrays);
    let table_schema = Arc::new(table_schema);
    let projected_schema = Arc::new(projected_schema);
    let filters = Arc::new(filters_to_evaluate);
    let filter_col_names = Arc::new(filter_col_names_to_load);

    let semaphore = Arc::new(Semaphore::new(16)); // max 16 chunks in flight

    let tasks: Vec<_> = chunk_tasks
        .into_iter()
        .map(|(chunk_indices, row_indices)| {
            let arrays = arrays.clone();
            let table_schema = table_schema.clone();
            let projected_schema = projected_schema.clone();
            let filters = filters.clone();
            let filter_col_names = filter_col_names.clone();
            let semaphore = semaphore.clone();

            tokio::task::spawn(async move {
                let _permit = semaphore
                    .acquire()
                    .await
                    .map_err(|_| ZarrDataFusionError::Custom("Semaphore closed".into()))?;

                if let Some(rows) = row_indices {
                    // Spatial index provided specific rows to extract
                    process_chunk_with_row_filter_async(
                        &arrays,
                        &table_schema,
                        &projected_schema,
                        &filters,
                        &filter_col_names,
                        &chunk_indices,
                        &rows,
                    )
                    .await
                } else {
                    // Normal chunk processing with filters
                    process_chunk_async(
                        &arrays,
                        &table_schema,
                        &projected_schema,
                        &filters,
                        &filter_col_names,
                        &chunk_indices,
                    )
                    .await
                }
            })
        })
        .collect();

    // Await all tasks, propagating any errors.
    let mut batches = Vec::new();
    for task in tasks.into_iter() {
        match task.await {
            Ok(Ok(Some(batch))) => {
                batches.push(batch);
            }
            Ok(Ok(None)) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => {
                return Err(ZarrDataFusionError::Custom(format!(
                    "chunk task panicked: {e}"
                )));
            }
        }
    }
    Ok(batches)
}

/// Process a chunk but only extract specific row indices
///
/// This is used when spatial index has pre-filtered which rows to include
async fn process_chunk_with_row_filter_async(
    arrays: &HashMap<String, Array<Arc<dyn AsyncReadableListableStorageTraits>>>,
    table_schema: &SchemaRef,
    projected_schema: &SchemaRef,
    filters: &[Expr],
    filter_col_names: &HashSet<String>,
    chunk_indices: &[u64],
    row_indices: &[u64],
) -> ZarrDataFusionResult<Option<RecordBatch>> {
    use arrow::array::UInt64Array;
    use arrow::compute::take;
    use std::collections::HashMap as StdHashMap;

    if row_indices.is_empty() {
        return Ok(None);
    }

    // Step 1: Determine final rows to extract and cache any pre-loaded filter columns
    let (final_row_indices, already_loaded) = if filters.is_empty() {
        // Spatial index handled all filters - use row_indices as-is
        (row_indices.to_vec(), StdHashMap::new())
    } else {
        // Need to evaluate additional filters on candidate rows
        let indices_array = UInt64Array::from(row_indices.to_vec());
        let mut filter_arrays = Vec::new();

        // Load filter columns and extract candidate rows
        for col_name in filter_col_names {
            let zarr_array = arrays.get(col_name).ok_or_else(|| {
                ZarrDataFusionError::Custom(format!("No open array for filter column '{col_name}'"))
            })?;
            let field = table_schema.field_with_name(col_name)?;
            let full_chunk_array =
                retrieve_chunk_as_arrow_async(zarr_array, field, chunk_indices).await?;
            let candidate_array = take(full_chunk_array.as_ref(), &indices_array, None)?;
            filter_arrays.push((col_name.clone(), candidate_array));
        }

        // Evaluate filters (e.g., precise geometry checks after bbox filtering)
        let bool_mask = evaluate_filters(filters, &filter_arrays)?;

        if bool_mask.true_count() == 0 {
            return Ok(None);
        }

        // Compute final row indices and filter the already-loaded columns
        let final_indices: Vec<u64> = row_indices
            .iter()
            .zip(bool_mask.iter())
            .filter_map(|(idx, pass)| if pass? { Some(*idx) } else { None })
            .collect();

        let mut already_loaded = StdHashMap::new();
        for (name, arr) in filter_arrays {
            let filtered = arrow::compute::filter(arr.as_ref(), &bool_mask)?;
            already_loaded.insert(name, filtered);
        }

        (final_indices, already_loaded)
    };

    // Step 2: Load projection columns (reusing cached filter columns where available)
    let final_indices_array = UInt64Array::from(final_row_indices);
    let mut output_arrays: Vec<ArrayRef> = Vec::new();

    for field in projected_schema.fields() {
        let col_name = field.name();

        let output_array = if let Some(arr) = already_loaded.get(col_name) {
            // Already loaded and filtered
            arr.clone()
        } else {
            // Load and extract using final indices (single take operation)
            let zarr_array = arrays.get(col_name).ok_or_else(|| {
                ZarrDataFusionError::Custom(format!(
                    "No open array for projected column '{col_name}'"
                ))
            })?;
            let table_field = table_schema.field_with_name(col_name)?;
            let full_chunk_array =
                retrieve_chunk_as_arrow_async(zarr_array, table_field, chunk_indices).await?;
            take(full_chunk_array.as_ref(), &final_indices_array, None)?
        };

        output_arrays.push(output_array);
    }

    Ok(Some(RecordBatch::try_new(
        projected_schema.clone(),
        output_arrays,
    )?))
}

async fn process_chunk_async(
    arrays: &HashMap<String, Array<Arc<dyn AsyncReadableListableStorageTraits>>>,
    table_schema: &SchemaRef,
    projected_schema: &SchemaRef,
    filters: &[Expr],
    filter_col_names: &HashSet<String>,
    chunk_indices: &[u64],
) -> ZarrDataFusionResult<Option<RecordBatch>> {
    // If there are no filters, load projected columns directly
    if filters.is_empty() {
        let mut output_arrays: Vec<ArrayRef> = Vec::new();

        for field in projected_schema.fields() {
            let col_name = field.name();

            let zarr_array = arrays.get(col_name).ok_or_else(|| {
                ZarrDataFusionError::Custom(format!(
                    "No open array for projected column '{col_name}'"
                ))
            })?;
            let table_field = table_schema.field_with_name(col_name)?;
            output_arrays
                .push(retrieve_chunk_as_arrow_async(zarr_array, table_field, chunk_indices).await?);
        }

        let batch = RecordBatch::try_new(projected_schema.clone(), output_arrays)?;
        return Ok(Some(batch));
    }

    // Read filter columns
    let mut filter_arrays: Vec<(String, ArrayRef)> = Vec::new();
    for col_name in filter_col_names {
        let zarr_array = arrays.get(col_name).ok_or_else(|| {
            ZarrDataFusionError::Custom(format!("No open array for filter column '{col_name}'"))
        })?;
        let field = table_schema.field_with_name(col_name)?;
        let array = retrieve_chunk_as_arrow_async(zarr_array, field, chunk_indices).await?;
        filter_arrays.push((col_name.clone(), array));
    }

    // Evaluate predicate
    let bool_mask = evaluate_filters(filters, &filter_arrays)?;
    let matches = bool_mask.true_count();

    // Skip chunk if no rows pass
    if matches == 0 {
        return Ok(None);
    }

    // Load projection columns (reusing filter data where it overlaps), apply mask
    let mut output_arrays: Vec<ArrayRef> = Vec::new();

    for field in projected_schema.fields() {
        let col_name = field.name();

        let full_array = if let Some((_, arr)) = filter_arrays.iter().find(|(n, _)| n == col_name) {
            arr.clone()
        } else {
            let zarr_array = arrays.get(col_name).ok_or_else(|| {
                ZarrDataFusionError::Custom(format!(
                    "No open array for projected column '{col_name}'"
                ))
            })?;
            let table_field = table_schema.field_with_name(col_name)?;
            retrieve_chunk_as_arrow_async(zarr_array, table_field, chunk_indices).await?
        };
        let filtered = arrow::compute::filter(full_array.as_ref(), &bool_mask)?;
        output_arrays.push(filtered);
    }

    Ok(Some(RecordBatch::try_new(
        projected_schema.clone(),
        output_arrays,
    )?))
}

/// Convert Vec<Vec<u8>> to a binary Arrow array based on the specified type.
fn binary_vec_to_arrow(data: Vec<Vec<u8>>, data_type: &DataType) -> ArrayRef {
    // Use from_iter with Some() wrapper for efficient conversion
    match data_type {
        DataType::Binary => {
            let array: BinaryArray = data.into_iter().map(Some).collect();
            Arc::new(array)
        }
        DataType::LargeBinary => {
            let array: LargeBinaryArray = data.into_iter().map(Some).collect();
            Arc::new(array)
        }
        DataType::BinaryView => {
            let array: BinaryViewArray = data.into_iter().map(Some).collect();
            Arc::new(array)
        }
        _ => unreachable!(
            "binary_vec_to_arrow called with non-binary type: {:?}",
            data_type
        ),
    }
}

/// Convert Vec<i64> to a timestamp Arrow array based on the time unit.
fn i64_vec_to_timestamp_arrow(data: Vec<i64>, unit: &TimeUnit) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(data)),
        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from(data)),
        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from(data)),
        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from(data)),
    }
}

async fn retrieve_chunk_as_arrow_async<S: AsyncReadableListableStorageTraits + 'static>(
    array: &Array<S>,
    field: &Field,
    chunk_indices: &[u64],
) -> ZarrDataFusionResult<ArrayRef> {
    let subset = array.chunk_subset_bounded(chunk_indices).map_err(|_| {
        ZarrDataFusionError::Custom(format!(
            "Invalid chunk indices {:?} for array at path {}",
            chunk_indices,
            array.path()
        ))
    })?;

    let data_type = field.data_type();
    match data_type {
        DataType::Boolean => {
            let data: Vec<bool> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(BooleanArray::from(data)))
        }
        DataType::Int8 => {
            let data: Vec<i8> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(Int8Array::from(data)))
        }
        DataType::Int16 => {
            let data: Vec<i16> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(Int16Array::from(data)))
        }
        DataType::Int32 => {
            let data: Vec<i32> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(Int32Array::from(data)))
        }
        DataType::Int64 => {
            let data: Vec<i64> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(Int64Array::from(data)))
        }
        DataType::UInt8 => {
            let data: Vec<u8> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(UInt8Array::from(data)))
        }
        DataType::UInt16 => {
            let data: Vec<u16> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(UInt16Array::from(data)))
        }
        DataType::UInt32 => {
            let data: Vec<u32> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(UInt32Array::from(data)))
        }
        DataType::UInt64 => {
            let data: Vec<u64> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(UInt64Array::from(data)))
        }
        DataType::Float32 => {
            let data: Vec<f32> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(Float32Array::from(data)))
        }
        DataType::Float64 => {
            let data: Vec<f64> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(Float64Array::from(data)))
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let data: Vec<Vec<u8>> = array.async_retrieve_array_subset_elements(&subset).await?;
            let arrow_array = binary_vec_to_arrow(data, data_type);
            Ok(arrow_array)
        }
        DataType::Utf8 => {
            let data: Vec<String> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(StringArray::from(data)))
        }
        DataType::LargeUtf8 => {
            let data: Vec<String> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(LargeStringArray::from(data)))
        }
        DataType::Utf8View => {
            let data: Vec<String> = array.async_retrieve_array_subset_elements(&subset).await?;
            Ok(Arc::new(StringViewArray::from(data)))
        }
        DataType::Timestamp(unit, _) => {
            let data: Vec<i64> = array.async_retrieve_array_subset_elements(&subset).await?;
            let arrow_array = i64_vec_to_timestamp_arrow(data.clone(), unit);
            Ok(arrow_array)
        }
        _ => Err(ZarrDataFusionError::Custom(format!(
            "Unsupported Arrow data type: {:?}",
            data_type
        ))),
    }
}

fn collect_columns_from_expr(expr: &Expr, out: &mut HashSet<String>) {
    use datafusion::logical_expr::Expr::*;
    match expr {
        Column(col) => {
            out.insert(col.name.clone());
        }
        BinaryExpr(b) => {
            collect_columns_from_expr(&b.left, out);
            collect_columns_from_expr(&b.right, out);
        }
        Not(inner) => collect_columns_from_expr(inner, out),
        IsNull(inner) | IsNotNull(inner) => collect_columns_from_expr(inner, out),
        IsTrue(inner) | IsFalse(inner) | IsUnknown(inner) | IsNotTrue(inner)
        | IsNotFalse(inner) | IsNotUnknown(inner) => collect_columns_from_expr(inner, out),
        Between(b) => {
            collect_columns_from_expr(&b.expr, out);
            collect_columns_from_expr(&b.low, out);
            collect_columns_from_expr(&b.high, out);
        }
        InList(il) => collect_columns_from_expr(&il.expr, out),
        Cast(c) => collect_columns_from_expr(&c.expr, out),
        TryCast(c) => collect_columns_from_expr(&c.expr, out),
        ScalarFunction(sf) => {
            for arg in &sf.args {
                collect_columns_from_expr(arg, out);
            }
        }
        _ => {}
    }
}

fn evaluate_filters(
    filters: &[Expr],
    filter_arrays: &[(String, ArrayRef)],
) -> ZarrDataFusionResult<BooleanArray> {
    if filters.is_empty() || filter_arrays.is_empty() {
        let len = filter_arrays.first().map(|(_, a)| a.len()).unwrap_or(0);
        return Ok(BooleanArray::from(vec![true; len]));
    }

    let filter_fields: Vec<Field> = filter_arrays
        .iter()
        .map(|(name, arr)| Field::new(name, arr.data_type().clone(), true))
        .collect();
    let filter_schema = Arc::new(Schema::new(filter_fields));
    let filter_batch = RecordBatch::try_new(
        filter_schema.clone(),
        filter_arrays.iter().map(|(_, a)| a.clone()).collect(),
    )?;

    let combined = filters
        .iter()
        .cloned()
        .reduce(|a, b| {
            Expr::BinaryExpr(LogicalBinaryExpr {
                left: Box::new(a),
                right: Box::new(b),
                op: Operator::And,
            })
        })
        .unwrap();

    let df_schema = filter_schema.clone().to_dfschema()?;
    let phys_expr = create_physical_expr(&combined, &df_schema, &ExecutionProps::new())?;

    match phys_expr.evaluate(&filter_batch)? {
        ColumnarValue::Array(arr) => arr
            .as_any()
            .downcast_ref::<BooleanArray>()
            .cloned()
            .ok_or_else(|| {
                ZarrDataFusionError::Custom("Filter did not evaluate to BooleanArray".into())
            }),
        ColumnarValue::Scalar(s) => {
            let len = filter_batch.num_rows();
            s.to_array_of_size(len)?
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| {
                    ZarrDataFusionError::Custom("Scalar filter did not cast to BooleanArray".into())
                })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::utils::get_local_zarr_store;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_basic_table_provider() {
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("zarr_table", Arc::new(provider))
            .unwrap();
        let df = ctx.sql("SELECT * FROM zarr_table").await.unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_table_provider_with_sql() {
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("zarr_table", Arc::new(provider))
            .unwrap();
        let df = ctx
            .sql("SELECT collection, date FROM zarr_table WHERE collection = 'collection_a'")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].num_columns(), 2);
        let collection_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(collection_col.value(0), "collection_a");
    }

    #[tokio::test]
    async fn test_st_intersects_selects_matching_record() {
        use arrow_array::Array;
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");
        let sql = "
            SELECT collection FROM zarr_data
            WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'))
            ORDER BY collection
        ";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);
        let collection_array = batches[0]
            .column_by_name("collection")
            .unwrap()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let collections: Vec<&str> = (0..collection_array.len())
            .map(|i| collection_array.value(i))
            .collect();
        assert!(collections.contains(&"collection_a"));
    }

    #[tokio::test]
    async fn test_st_intersects_no_match() {
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");
        let sql = "
            SELECT collection FROM zarr_data
            WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((100 100, 100 110, 110 110, 110 100, 100 100))'))
        ";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_st_intersects_multiple_matches() {
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");
        let sql = "
            SELECT collection FROM zarr_data
            WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((-15 -15, -15 15, 15 15, 15 -15, -15 -15))'))
            ORDER BY collection
        ";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() >= 2);
    }

    #[tokio::test]
    async fn test_st_intersects_with_bbox_in_projection() {
        use arrow_array::Array;
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");
        // Select bbox in the projection along with the filter
        let sql = "
            SELECT bbox, collection FROM zarr_data
            WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'))
            ORDER BY collection
        ";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);

        // Verify we have both bbox and collection columns
        assert_eq!(batches[0].num_columns(), 2);
        assert!(batches[0].column_by_name("bbox").is_some());
        assert!(batches[0].column_by_name("collection").is_some());

        // Verify bbox column has data
        let bbox_col = batches[0].column_by_name("bbox").unwrap();
        assert_eq!(bbox_col.len(), batches[0].num_rows());
    }

    #[tokio::test]
    async fn test_st_intersects_with_geoindex() {
        let wrapper = get_local_zarr_store(true).await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");

        // Query that should use the R-tree index at /indexes/bbox
        let sql = "
            SELECT collection FROM zarr_data
            WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'))
            ORDER BY collection
        ";
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);
    }

    #[tokio::test]
    async fn test_st_intersects_with_geoarrow_literal() {
        use datafusion::execution::FunctionRegistry;
        use datafusion::logical_expr::{Expr, col};
        use datafusion::scalar::ScalarValue;
        use geoarrow_array::GeoArrowArray;

        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let local_fs = LocalFileSystem::new_with_prefix(path).unwrap();
        let provider = ZarrTableProvider::new_object_store(local_fs, "/meta")
            .await
            .unwrap();
        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");

        // Create a GeoArrow polygon: POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))
        // Use geo crate to create the polygon, then convert to GeoArrow
        let coords: Vec<geo::Coord> = vec![
            geo::coord! { x: 0.0, y: 0.0 },
            geo::coord! { x: 0.0, y: 5.0 },
            geo::coord! { x: 5.0, y: 5.0 },
            geo::coord! { x: 5.0, y: 0.0 },
            geo::coord! { x: 0.0, y: 0.0 },
        ];
        let polygon = geo::Polygon::new(geo::LineString::from(coords), vec![]);

        // Convert to GeoArrow using the WKT conversion path
        use wkb::writer::{WriteOptions, write_polygon};
        let mut wkb_buffer = Vec::new();
        write_polygon(&mut wkb_buffer, &polygon, &WriteOptions::default()).unwrap();

        // Create WKB array then convert to geometry
        use geoarrow_array::array::WkbArray;
        use geoarrow_schema::Crs;
        let crs = Crs::from_authority_code("EPSG:4326".to_string());
        let metadata = Arc::new(geoarrow_schema::Metadata::new(crs, None));
        let wkb_array = WkbArray::new(vec![wkb_buffer.as_slice()].into(), metadata);

        // Convert to Arrow array and then to ScalarValue
        let arrow_array = wkb_array.into_array_ref();
        let scalar = ScalarValue::try_from_array(&arrow_array, 0).unwrap();

        // Build query using DataFusion expr API: ST_Intersects(bbox, polygon_literal)
        let st_intersects_fn = ctx
            .udf("st_intersects")
            .expect("ST_Intersects function not found");

        let filter_expr = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction {
            func: st_intersects_fn,
            args: vec![col("bbox"), Expr::Literal(scalar, None)],
        });

        // Execute query with GeoArrow literal
        let df = ctx
            .table("zarr_data")
            .await
            .unwrap()
            .filter(filter_expr)
            .unwrap()
            .select(vec![col("collection")])
            .unwrap()
            .sort(vec![col("collection").sort(true, false)])
            .unwrap();

        let batches = df.collect().await.unwrap();
        assert!(!batches.is_empty());
        assert!(batches[0].num_rows() > 0);
    }
}
