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
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use zarrs::array::{Array, ElementOwned};
use zarrs::array_subset::ArraySubset;
use zarrs::group::Group;
use zarrs::storage::{AsyncReadableListableStorageTraits, ReadableListableStorageTraits};
use zarrs_filesystem::{FilesystemStore, FilesystemStoreCreateError};
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_storage::{MaybeSend, MaybeSync};

use crate::error::{ZarrDataFusionError, ZarrDataFusionResult};
use crate::schema::{group_arrays_schema, group_arrays_schema_async};

pub fn register_spatial_functions(ctx: &SessionContext) -> Result<()> {
    geodatafusion::register(ctx);
    Ok(())
}

#[derive(Debug)]
pub struct ZarrTableProvider {
    schema: SchemaRef,
    zarr_backend: ZarrBackend,
    group_path: String,
}

impl ZarrTableProvider {
    pub fn new_filesystem<P: AsRef<std::path::Path>>(
        base_path: P,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = SyncZarrBackend::new_filesystem(base_path)?;
        let schema = zarr_backend.infer_group_schema(group_path)?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
            group_path: group_path.to_string(),
        })
    }

    pub async fn new_icechunk(
        icechunk_session: icechunk::session::Session,
        handle: Handle,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let store = Arc::new(AsyncIcechunkStore::new(icechunk_session));
        let zarr_backend = IcechunkZarrBackend { store, handle };
        let schema = zarr_backend.infer_group_schema(group_path).await?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
            group_path: group_path.to_string(),
        })
    }

    pub async fn new_object_store<T: ObjectStore>(
        store: T,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = AsyncZarrBackend::new_object_store(store);
        let schema = zarr_backend.infer_group_schema(group_path).await?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
            group_path: group_path.to_string(),
        })
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
            .map(|_| TableProviderFilterPushDown::Inexact)
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
        )))
    }
}

#[derive(Clone)]
struct SyncZarrBackend(Arc<dyn ReadableListableStorageTraits>);

impl SyncZarrBackend {
    fn new_filesystem<P: AsRef<std::path::Path>>(
        base_path: P,
    ) -> Result<Self, FilesystemStoreCreateError> {
        Ok(SyncZarrBackend(Arc::new(FilesystemStore::new(base_path)?)))
    }

    fn load_array<T: ElementOwned>(&self, path: &str) -> ZarrDataFusionResult<Vec<T>> {
        let array = Array::open(self.0.clone(), path)?;
        let full_subset = ArraySubset::new_with_shape(array.shape().to_vec());
        Ok(array.retrieve_array_subset_elements(&full_subset)?)
    }

    fn infer_group_schema(&self, group_path: &str) -> ZarrDataFusionResult<SchemaRef> {
        let group = Group::open(self.0.clone(), group_path)?;
        group_arrays_schema(&group)
    }
}

#[derive(Clone)]
struct IcechunkZarrBackend {
    store: Arc<dyn AsyncReadableListableStorageTraits>,
    handle: Handle,
}

impl IcechunkZarrBackend {
    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync + 'static>(
        &self,
        path: &str,
    ) -> ZarrDataFusionResult<Vec<T>> {
        let array = Array::async_open(self.store.clone(), path).await?;
        let full_subset = ArraySubset::new_with_shape(array.shape().to_vec());
        Ok(array
            .async_retrieve_array_subset_elements(&full_subset)
            .await?)
    }

    async fn infer_group_schema(&self, group_path: &str) -> ZarrDataFusionResult<SchemaRef> {
        let group = Group::async_open(self.store.clone(), group_path).await?;
        group_arrays_schema_async(&group).await
    }
}

#[derive(Clone)]
struct AsyncZarrBackend(Arc<dyn AsyncReadableListableStorageTraits>);

impl AsyncZarrBackend {
    fn new_object_store<T: ObjectStore>(store: T) -> Self {
        AsyncZarrBackend(Arc::new(zarrs_object_store::AsyncObjectStore::new(store)))
    }

    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync, S: AsRef<str>>(
        &self,
        path: S,
    ) -> ZarrDataFusionResult<Vec<T>> {
        let array = Array::async_open(self.0.clone(), path.as_ref()).await?;
        let full_subset = ArraySubset::new_with_shape(array.shape().to_vec());
        Ok(array
            .async_retrieve_array_subset_elements(&full_subset)
            .await?)
    }

    async fn infer_group_schema(&self, group_path: &str) -> ZarrDataFusionResult<SchemaRef> {
        let group = Group::async_open(self.0.clone(), group_path).await?;
        group_arrays_schema_async(&group).await
    }
}

#[derive(Clone)]
enum ZarrBackend {
    Async(AsyncZarrBackend),
    Icechunk(IcechunkZarrBackend),
    Sync(SyncZarrBackend),
}

impl Debug for ZarrBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZarrBackend::Async(_) => write!(f, "ZarrBackend::Async"),
            ZarrBackend::Icechunk(_) => write!(f, "ZarrBackend::Icechunk"),
            ZarrBackend::Sync(_) => write!(f, "ZarrBackend::Sync"),
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

impl From<SyncZarrBackend> for ZarrBackend {
    fn from(b: SyncZarrBackend) -> Self {
        ZarrBackend::Sync(b)
    }
}

impl ZarrBackend {
    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync + 'static>(
        &self,
        path: &str,
    ) -> ZarrDataFusionResult<Vec<T>> {
        match self {
            ZarrBackend::Sync(b) => b.load_array(path),
            ZarrBackend::Icechunk(b) => b.load_array(path).await,
            ZarrBackend::Async(b) => b.load_array(path).await,
        }
    }

    async fn load_array_given_field(&self, field: &Field) -> ZarrDataFusionResult<ArrayRef> {
        let group = "/meta";
        let name = field.name();
        let path = format!("{group}/{name}");

        match field.data_type() {
            DataType::Boolean => {
                let data: Vec<bool> = self.load_array(&path).await?;
                Ok(Arc::new(BooleanArray::from(data)))
            }
            DataType::Int8 => {
                let data: Vec<i8> = self.load_array(&path).await?;
                Ok(Arc::new(Int8Array::from(data)))
            }
            DataType::Int16 => {
                let data: Vec<i16> = self.load_array(&path).await?;
                Ok(Arc::new(Int16Array::from(data)))
            }
            DataType::Int32 => {
                let data: Vec<i32> = self.load_array(&path).await?;
                Ok(Arc::new(Int32Array::from(data)))
            }
            DataType::Int64 => {
                let data: Vec<i64> = self.load_array(&path).await?;
                Ok(Arc::new(Int64Array::from(data)))
            }
            DataType::UInt8 => {
                let data: Vec<u8> = self.load_array(&path).await?;
                Ok(Arc::new(UInt8Array::from(data)))
            }
            DataType::UInt16 => {
                let data: Vec<u16> = self.load_array(&path).await?;
                Ok(Arc::new(UInt16Array::from(data)))
            }
            DataType::UInt32 => {
                let data: Vec<u32> = self.load_array(&path).await?;
                Ok(Arc::new(UInt32Array::from(data)))
            }
            DataType::UInt64 => {
                let data: Vec<u64> = self.load_array(&path).await?;
                Ok(Arc::new(UInt64Array::from(data)))
            }
            DataType::Float32 => {
                let data: Vec<f32> = self.load_array(&path).await?;
                Ok(Arc::new(Float32Array::from(data)))
            }
            DataType::Float64 => {
                let data: Vec<f64> = self.load_array(&path).await?;
                Ok(Arc::new(Float64Array::from(data)))
            }
            DataType::Binary => {
                let data: Vec<Vec<u8>> = self.load_array(&path).await?;
                let refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
                Ok(Arc::new(BinaryArray::from(refs)))
            }
            DataType::LargeBinary => {
                let data: Vec<Vec<u8>> = self.load_array(&path).await?;
                let refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
                Ok(Arc::new(LargeBinaryArray::from(refs)))
            }
            DataType::BinaryView => {
                let data: Vec<Vec<u8>> = self.load_array(&path).await?;
                let refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
                Ok(Arc::new(BinaryViewArray::from(refs)))
            }
            DataType::Utf8 => {
                let data: Vec<String> = self.load_array(&path).await?;
                Ok(Arc::new(StringArray::from(data)))
            }
            DataType::LargeUtf8 => {
                let data: Vec<String> = self.load_array(&path).await?;
                Ok(Arc::new(LargeStringArray::from(data)))
            }
            DataType::Utf8View => {
                let data: Vec<String> = self.load_array(&path).await?;
                Ok(Arc::new(StringViewArray::from(data)))
            }
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Millisecond => {
                    let data: Vec<i64> = self.load_array(&path).await?;
                    Ok(Arc::new(TimestampMillisecondArray::from(data)))
                }
                TimeUnit::Microsecond => {
                    let data: Vec<i64> = self.load_array(&path).await?;
                    Ok(Arc::new(TimestampMicrosecondArray::from(data)))
                }
                TimeUnit::Nanosecond => {
                    let data: Vec<i64> = self.load_array(&path).await?;
                    Ok(Arc::new(TimestampNanosecondArray::from(data)))
                }
                TimeUnit::Second => {
                    let data: Vec<i64> = self.load_array(&path).await?;
                    Ok(Arc::new(TimestampSecondArray::from(data)))
                }
            },
            _ => Err(ZarrDataFusionError::Custom(format!(
                "Unsupported Arrow data type: {:?}",
                field.data_type()
            ))),
        }
    }

    async fn load_record_batch(self, schema: SchemaRef) -> ZarrDataFusionResult<RecordBatch> {
        let mut arrays = vec![];
        for field in schema.fields() {
            arrays.push(self.load_array_given_field(field).await?);
        }
        Ok(RecordBatch::try_new(schema.clone(), arrays)?)
    }
}

#[derive(Debug)]
struct ZarrExec {
    zarr_backend: ZarrBackend,
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    group_path: String,
    properties: PlanProperties,
}

impl ZarrExec {
    fn new(
        zarr_backend: ZarrBackend,
        table_schema: SchemaRef,
        projected_schema: SchemaRef,
        filters: Vec<Expr>,
        group_path: String,
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

        match &backend {
            ZarrBackend::Sync(sync_backend) => {
                let sync_backend = sync_backend.clone();
                let stream = RecordBatchStreamAdapter::new(
                    self.projected_schema.clone(),
                    futures::stream::once(async move {
                        tokio::task::spawn_blocking(move || {
                            scan_chunks_sync(
                                &sync_backend,
                                &group_path,
                                table_schema,
                                projected_schema,
                                filters,
                                filter_col_names,
                            )
                        })
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                        .map_err(|e: ZarrDataFusionError| {
                            datafusion::error::DataFusionError::from(e)
                        })
                    })
                    .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                    .try_flatten(),
                );
                Ok(Box::pin(stream))
            }

            ZarrBackend::Icechunk(icechunk_backend) => {
                let store = icechunk_backend.store.clone();
                let handle = icechunk_backend.handle.clone();
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
                            ))
                            .await
                            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                            .map_err(|e: ZarrDataFusionError| {
                                datafusion::error::DataFusionError::from(e)
                            })
                    })
                    .map_ok(|batches| futures::stream::iter(batches.into_iter().map(Ok)))
                    .try_flatten(),
                );
                Ok(Box::pin(stream))
            }

            ZarrBackend::Async(_) => {
                let projected_schema = self.projected_schema.clone();
                let stream = RecordBatchStreamAdapter::new(
                    projected_schema.clone(),
                    futures::stream::once(async move {
                        backend.load_record_batch(projected_schema).await.map_err(
                            |e: ZarrDataFusionError| datafusion::error::DataFusionError::from(e),
                        )
                    }),
                );
                Ok(Box::pin(stream))
            }
        }
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

fn scan_chunks_sync(
    backend: &SyncZarrBackend,
    group: &str,
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    filter_col_names: HashSet<String>,
) -> ZarrDataFusionResult<Vec<RecordBatch>> {
    let all_cols: HashSet<String> = filter_col_names
        .iter()
        .chain(projected_schema.fields().iter().map(|f| f.name()))
        .cloned()
        .collect();

    let arrays: HashMap<String, Array<Arc<dyn ReadableListableStorageTraits>>> = all_cols
        .iter()
        .map(|col_name| {
            let path = format!("{group}/{col_name}");
            let array = Array::<Arc<dyn ReadableListableStorageTraits>>::open(
                Arc::new(backend.0.clone()),
                &path,
            )?;
            Ok((col_name.clone(), array))
        })
        .collect::<ZarrDataFusionResult<_>>()?;

    let chunk_grid_shape = match arrays.values().next() {
        Some(a) => a.chunk_grid_shape(),
        None => return Err(ZarrDataFusionError::Custom("No arrays to scan".into())),
    };

    let chunks = ArraySubset::new_with_ranges(&chunk_grid_shape.iter().map(|&n| 0..n).collect::<Vec<_>>());
    let indices = chunks.indices();

    indices.into_par_iter()
        .filter_map(|chunk_indices| {
            process_chunk(
                &arrays,
                &table_schema,
                &projected_schema,
                &filters,
                &filter_col_names,
                &chunk_indices,
            )
            .transpose()
        })
        .collect::<ZarrDataFusionResult<Vec<_>>>()
}

async fn scan_chunks_async(
    store: Arc<dyn AsyncReadableListableStorageTraits>,
    group: String,
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    filter_col_names: HashSet<String>,
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
        let array = Array::<Arc<dyn AsyncReadableListableStorageTraits>>::async_open(
            Arc::new(store.clone()),
            &path,
        )
        .await?;
        arrays.insert(col_name.clone(), array);
    }

    let chunk_grid_shape = match arrays.values().next() {
        Some(a) => a.chunk_grid_shape(),
        None => return Err(ZarrDataFusionError::Custom("No arrays to scan".into())),
    };

    let ranges: Vec<std::ops::Range<u64>> = chunk_grid_shape.iter().map(|&n| 0..n).collect();

    // Wrap arrays in Arc so they can be shared across spawned tasks.
    let arrays = Arc::new(arrays);
    let table_schema = Arc::new(table_schema);
    let projected_schema = Arc::new(projected_schema);
    let filters = Arc::new(filters);
    let filter_col_names = Arc::new(filter_col_names);

    let semaphore = Arc::new(Semaphore::new(16)); // max 16 chunks in flight

    let tasks: Vec<_> = ArraySubset::new_with_ranges(&ranges)
        .indices()
        .into_iter()
        .map(|chunk_indices| {
            let arrays = arrays.clone();
            let table_schema = table_schema.clone();
            let projected_schema = projected_schema.clone();
            let filters = filters.clone();
            let filter_col_names = filter_col_names.clone();
            let semaphore = semaphore.clone();

            tokio::spawn(async move {
                let _permit = semaphore.acquire().await.unwrap();
                process_chunk_async(
                    &arrays,
                    &table_schema,
                    &projected_schema,
                    &filters,
                    &filter_col_names,
                    &chunk_indices,
                )
                .await
            })
        })
        .collect();

    // Await all tasks, propagating any errors.
    let mut batches = Vec::new();
    for task in tasks {
        match task.await {
            Ok(Ok(Some(batch))) => batches.push(batch),
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

fn process_chunk(
    arrays: &HashMap<String, Array<Arc<dyn ReadableListableStorageTraits>>>,
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
            output_arrays.push(retrieve_chunk_as_arrow(
                zarr_array,
                table_field,
                chunk_indices,
            )?);
        }
        return Ok(Some(RecordBatch::try_new(
            projected_schema.clone(),
            output_arrays,
        )?));
    }

    // Phase 1: read filter columns
    let mut filter_arrays: Vec<(String, ArrayRef)> = Vec::new();
    for col_name in filter_col_names {
        let zarr_array = arrays.get(col_name).ok_or_else(|| {
            ZarrDataFusionError::Custom(format!("No open array for filter column '{col_name}'"))
        })?;
        let field = table_schema.field_with_name(col_name)?;
        filter_arrays.push((
            col_name.clone(),
            retrieve_chunk_as_arrow(zarr_array, field, chunk_indices)?,
        ));
    }

    // Phase 2: evaluate predicate
    let bool_mask = evaluate_filters(filters, &filter_arrays)?;

    // Phase 3: skip chunk if no rows pass
    if bool_mask.true_count() == 0 {
        return Ok(None);
    }

    // Phase 4: load projection columns (reusing filter data where it overlaps), apply mask
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
            retrieve_chunk_as_arrow(zarr_array, table_field, chunk_indices)?
        };
        output_arrays.push(arrow::compute::filter(full_array.as_ref(), &bool_mask)?);
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
        return Ok(Some(RecordBatch::try_new(
            projected_schema.clone(),
            output_arrays,
        )?));
    }

    // Phase 1: read filter columns
    let mut filter_arrays: Vec<(String, ArrayRef)> = Vec::new();
    for col_name in filter_col_names {
        let zarr_array = arrays.get(col_name).ok_or_else(|| {
            ZarrDataFusionError::Custom(format!("No open array for filter column '{col_name}'"))
        })?;
        let field = table_schema.field_with_name(col_name)?;
        filter_arrays.push((
            col_name.clone(),
            retrieve_chunk_as_arrow_async(zarr_array, field, chunk_indices).await?,
        ));
    }

    // Phase 2: evaluate predicate
    let bool_mask = evaluate_filters(filters, &filter_arrays)?;

    // Phase 3: skip chunk if no rows pass
    if bool_mask.true_count() == 0 {
        return Ok(None);
    }

    // Phase 4: load projection columns (reusing filter data where it overlaps), apply mask
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
        output_arrays.push(arrow::compute::filter(full_array.as_ref(), &bool_mask)?);
    }

    Ok(Some(RecordBatch::try_new(
        projected_schema.clone(),
        output_arrays,
    )?))
}

/// Convert Vec<Vec<u8>> to a binary Arrow array based on the specified type.
fn binary_vec_to_arrow(data: Vec<Vec<u8>>, data_type: &DataType) -> ArrayRef {
    let refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();
    match data_type {
        DataType::Binary => Arc::new(BinaryArray::from(refs)),
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(refs)),
        DataType::BinaryView => Arc::new(BinaryViewArray::from(refs)),
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

fn retrieve_chunk_as_arrow<S: ReadableListableStorageTraits + 'static>(
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
            let data: Vec<bool> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(BooleanArray::from(data)))
        }
        DataType::Int8 => {
            let data: Vec<i8> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(Int8Array::from(data)))
        }
        DataType::Int16 => {
            let data: Vec<i16> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(Int16Array::from(data)))
        }
        DataType::Int32 => {
            let data: Vec<i32> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(Int32Array::from(data)))
        }
        DataType::Int64 => {
            let data: Vec<i64> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(Int64Array::from(data)))
        }
        DataType::UInt8 => {
            let data: Vec<u8> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(UInt8Array::from(data)))
        }
        DataType::UInt16 => {
            let data: Vec<u16> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(UInt16Array::from(data)))
        }
        DataType::UInt32 => {
            let data: Vec<u32> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(UInt32Array::from(data)))
        }
        DataType::UInt64 => {
            let data: Vec<u64> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(UInt64Array::from(data)))
        }
        DataType::Float32 => {
            let data: Vec<f32> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(Float32Array::from(data)))
        }
        DataType::Float64 => {
            let data: Vec<f64> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(Float64Array::from(data)))
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            let data: Vec<Vec<u8>> = array.retrieve_array_subset_elements(&subset)?;
            Ok(binary_vec_to_arrow(data, data_type))
        }
        DataType::Utf8 => {
            let data: Vec<String> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(StringArray::from(data)))
        }
        DataType::LargeUtf8 => {
            let data: Vec<String> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(LargeStringArray::from(data)))
        }
        DataType::Utf8View => {
            let data: Vec<String> = array.retrieve_array_subset_elements(&subset)?;
            Ok(Arc::new(StringViewArray::from(data)))
        }
        DataType::Timestamp(unit, _) => {
            let data: Vec<i64> = array.retrieve_array_subset_elements(&subset)?;
            Ok(i64_vec_to_timestamp_arrow(data, unit))
        }
        _ => Err(ZarrDataFusionError::Custom(format!(
            "Unsupported Arrow data type: {:?}",
            data_type
        ))),
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
            Ok(binary_vec_to_arrow(data, data_type))
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
            Ok(i64_vec_to_timestamp_arrow(data, unit))
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
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();
        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();
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
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();
        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();
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
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();
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
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();
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
        use arrow_array::Array;
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();
        let ctx = SessionContext::new();
        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();
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
        assert!(collections.contains(&"collection_b"));
    }
}
