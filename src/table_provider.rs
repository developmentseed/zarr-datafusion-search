use arrow_array::{
    ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, StringViewArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
    UInt64Array,
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;
use tokio::runtime::Handle;
use zarrs::array::{Array, ElementOwned};
use zarrs::array_subset::ArraySubset;
use zarrs::group::Group;
use zarrs::storage::{AsyncReadableListableStorageTraits, ReadableListableStorageTraits};
use zarrs_filesystem::{FilesystemStore, FilesystemStoreCreateError};
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_storage::{MaybeSend, MaybeSync};

use std::collections::HashSet;

use arrow::compute::filter as arrow_filter_fn;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::physical_plan::ColumnarValue;
use datafusion::common::ToDFSchema;

use crate::error::{ZarrDataFusionError, ZarrDataFusionResult};
use crate::schema::{group_arrays_schema, group_arrays_schema_async};

pub fn register_spatial_functions(ctx: &SessionContext) -> Result<()> {
    geodatafusion::register(ctx);
    Ok(())
}

/// A simple DataFusion table provider that loads data from a Zarr store
#[derive(Debug)]
pub struct ZarrTableProvider {
    schema: SchemaRef,
    zarr_backend: ZarrBackend,
}

impl ZarrTableProvider {
    /// Create a new ZarrTableProvider from a Zarr store path
    pub fn new_filesystem<P: AsRef<std::path::Path>>(
        base_path: P,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = SyncZarrBackend::new_filesystem(base_path)?;
        let schema = zarr_backend.infer_group_schema(group_path)?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
        })
    }

    /// Create a new ZarrTableProvider from an Icechunk session
    pub async fn new_icechunk(
        icechunk_session: icechunk::session::Session,
        handle: Handle,
        group_path: impl Into<String>,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = IcechunkBackend::new(icechunk_session, handle);
        let schema = zarr_backend.infer_group_schema(group_path.into()).await?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
        })
    }

    /// Create a new ZarrTableProvider from an ObjectStore
    pub async fn new_object_store<T: ObjectStore>(
        store: T,
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = AsyncZarrBackend::new_object_store(store);
        let schema = zarr_backend.infer_group_schema(group_path).await?;
        Ok(Self {
            schema,
            zarr_backend: zarr_backend.into(),
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
        // Claim all filters as Inexact: we will apply them ourselves, but DataFusion
        // may still add a top-level filter node as a safety net.
        // Use Exact once you're confident in the evaluation correctness.
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
        // Build the projected schema from the requested column indices.
        // If no projection is requested, use the full schema.
        let projected_schema: SchemaRef = match projection {
            Some(indices) => Arc::new(self.schema.project(indices)?),
            None => self.schema.clone(),
        };

        Ok(Arc::new(ZarrExec::new(
            self.zarr_backend.clone(),
            self.schema.clone(),
            projected_schema,
            filters.to_vec(),
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
struct IcechunkBackend {
    store: Arc<dyn AsyncReadableListableStorageTraits>,
    handle: Handle,
}

impl IcechunkBackend {
    fn new(session: icechunk::session::Session, handle: Handle) -> Self {
        let store = Arc::new(AsyncIcechunkStore::new(session));
        Self { store, handle }
    }

    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync + 'static>(
        &self,
        path: String,
    ) -> ZarrDataFusionResult<Vec<T>> {
        let store = self.store.clone();
        self.handle
            .spawn(async move {
                let array = Array::async_open(store.clone(), path.as_ref()).await?;
                let full_subset = ArraySubset::new_with_shape(array.shape().to_vec());
                let out = array
                    .async_retrieve_array_subset_elements(&full_subset)
                    .await?;
                Ok::<_, ZarrDataFusionError>(out)
            })
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
    }

    async fn infer_group_schema(&self, group_path: String) -> ZarrDataFusionResult<SchemaRef> {
        let store = self.store.clone();
        self.handle
            .spawn(async move {
                let group = Group::async_open(store.clone(), &group_path).await?;
                group_arrays_schema_async(&group).await
            })
            .await
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
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
    Icechunk(IcechunkBackend),
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
    fn from(async_backend: AsyncZarrBackend) -> Self {
        ZarrBackend::Async(async_backend)
    }
}

impl From<IcechunkBackend> for ZarrBackend {
    fn from(icechunk_backend: IcechunkBackend) -> Self {
        ZarrBackend::Icechunk(icechunk_backend)
    }
}

impl From<SyncZarrBackend> for ZarrBackend {
    fn from(sync_backend: SyncZarrBackend) -> Self {
        ZarrBackend::Sync(sync_backend)
    }
}

impl ZarrBackend {
    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync + 'static>(
        &self,
        path: &str,
    ) -> ZarrDataFusionResult<Vec<T>> {
        match self {
            ZarrBackend::Sync(sync_backend) => sync_backend.load_array(path),
            ZarrBackend::Icechunk(icechunk_backend) => {
                icechunk_backend.load_array(path.to_string()).await
            }
            ZarrBackend::Async(async_backend) => async_backend.load_array(path).await,
        }
    }

    async fn load_array_given_field(&self, field: &Field) -> ZarrDataFusionResult<ArrayRef> {
        // Note: we don't need to check for extension type information here, because we're only
        // loading the physical data, and the metadata is already held in the schema.

        // TODO: refactor so this can be stored in the ZarrBackend
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
            // DataType::Float16 => {
            //     let data: Vec<f16> = self.load_array(&path).await?;
            //     Ok(Arc::new(Float16Array::from(data)))
            // }
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
    /// Full table schema (used to resolve column paths and build physical exprs)
    table_schema: SchemaRef,
    /// The schema that this node outputs — only the projected columns
    projected_schema: SchemaRef,
    /// Logical filter expressions pushed down from the planner
    filters: Vec<Expr>,
    properties: PlanProperties,
}

impl ZarrExec {
    fn new(
        zarr_backend: ZarrBackend,
        table_schema: SchemaRef,
        projected_schema: SchemaRef,
        filters: Vec<Expr>,
    ) -> Self {
        // PlanProperties are expressed in terms of the *output* schema
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
            properties,
        }
    }

    /// Collect the names of all columns referenced in the filter expressions.
    fn filter_column_names(filters: &[Expr]) -> HashSet<String> {
        let mut cols = HashSet::new();
        for expr in filters {
            collect_columns_from_expr(expr, &mut cols);
        }
        cols
    }
}

/// Recursively walk a logical Expr tree and collect referenced Column names.
fn collect_columns_from_expr(expr: &Expr, out: &mut HashSet<String>) {
    use datafusion::logical_expr::expr::Expr::*;
    match expr {
        Column(col) => { out.insert(col.name.clone()); }
        BinaryExpr(b) => {
            collect_columns_from_expr(&b.left, out);
            collect_columns_from_expr(&b.right, out);
        }
        Not(inner) => collect_columns_from_expr(inner, out),
        IsNull(inner) | IsNotNull(inner) => collect_columns_from_expr(inner, out),
        IsTrue(inner) | IsFalse(inner) | IsUnknown(inner)
        | IsNotTrue(inner) | IsNotFalse(inner) | IsNotUnknown(inner) => {
            collect_columns_from_expr(inner, out)
        }
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
        // Ignore literals, wildcards, etc.
        _ => {}
    }
}

/// Phase 1: scan only filter columns → evaluate predicate → boolean mask
/// Phase 2: scan projected columns (reusing filter arrays where they overlap)
/// Phase 3: apply mask to every column → emit RecordBatch
async fn load_filtered_batch(
    backend: ZarrBackend,
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    filters: Vec<Expr>,
    filter_col_names: HashSet<String>,
) -> ZarrDataFusionResult<RecordBatch> {

    // ── Phase 1: load filter columns ────────────────────────────────────────
    // We use the *table* schema to look up the correct field metadata / paths.
    let mut filter_arrays: Vec<(String, ArrayRef)> = Vec::new();

    for col_name in &filter_col_names {
        if let Ok(field) = table_schema.field_with_name(col_name) {
            let array = backend.load_array_given_field(field).await?;
            filter_arrays.push((col_name.clone(), array));
        }
    }

    // ── Phase 2: evaluate the filter predicate ───────────────────────────────
    let bool_mask: BooleanArray = if filters.is_empty() || filter_arrays.is_empty() {
        // No filter — keep everything; length comes from any array (or 0)
        let len = filter_arrays.first().map(|(_, a)| a.len()).unwrap_or(0);
        BooleanArray::from(vec![true; len])
    } else {
        // Build a mini RecordBatch containing only the filter columns so we can
        // evaluate the physical expression against it.
        let filter_fields: Vec<Field> = filter_arrays
            .iter()
            .map(|(name, arr)| Field::new(name, arr.data_type().clone(), true))
            .collect();
        let filter_schema = Arc::new(arrow_schema::Schema::new(filter_fields));
        let filter_batch = RecordBatch::try_new(
            filter_schema.clone(),
            filter_arrays.iter().map(|(_, a)| a.clone()).collect(),
        )?;

        // Combine all filter expressions with AND
        let combined = filters
            .into_iter()
            .reduce(|a, b| {
                datafusion::logical_expr::Expr::BinaryExpr(
                    datafusion::logical_expr::expr::BinaryExpr {
                        left: Box::new(a),
                        right: Box::new(b),
                        op: datafusion::logical_expr::Operator::And,
                    },
                )
            })
            .unwrap(); // safe: filters is non-empty

        // Convert the logical Expr into a physical expression evaluated against
        // the filter_schema (which only has the filter columns).
        let df_schema = filter_schema.clone().to_dfschema()?;
        let phys_expr =
            create_physical_expr(&combined, &df_schema, &ExecutionProps::new())?;

        let result = phys_expr.evaluate(&filter_batch)?;
        match result {
            ColumnarValue::Array(arr) => arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ZarrDataFusionError::Custom(
                        "Filter expression did not evaluate to a BooleanArray".into(),
                    )
                })?
                .clone(),
            ColumnarValue::Scalar(scalar) => {
                // Scalar boolean — replicate across all rows
                let len = filter_batch.num_rows();
                scalar
                    .to_array_of_size(len)?
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ZarrDataFusionError::Custom(
                            "Scalar filter did not cast to BooleanArray".into(),
                        )
                    })?
                    .clone()
            }
        }
    };

    // ── Phase 3: load projected columns and apply the mask ───────────────────
    let mut output_arrays: Vec<ArrayRef> = Vec::new();

    for field in projected_schema.fields() {
        let col_name = field.name();

        // Reuse what we already fetched during the filter phase where possible
        let full_array = if let Some((_, arr)) =
            filter_arrays.iter().find(|(n, _)| n == col_name)
        {
            arr.clone()
        } else {
            // Field wasn't in the filter — fetch it now using the table schema's
            // field definition (which has the correct metadata/path info)
            let table_field = table_schema.field_with_name(col_name)?;
            backend.load_array_given_field(table_field).await?
        };

        // Apply the boolean mask to select only matching rows
        let masked = arrow_filter_fn(full_array.as_ref(), &bool_mask)?;
        output_arrays.push(masked);
    }

    Ok(RecordBatch::try_new(projected_schema, output_arrays)?)
}
impl ExecutionPlan for ZarrExec {
    fn name(&self) -> &str {
        "ZarrExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        // Return projected schema — this is what flows to the parent operator
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

        let stream = RecordBatchStreamAdapter::new(
            projected_schema.clone(),
            futures::stream::once(async move {
                let batch = load_filtered_batch(
                    backend,
                    table_schema,
                    projected_schema,
                    filters,
                    filter_col_names,
                )
                .await?;
                Ok(batch)
            }),
        );
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for ZarrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ZarrExec: schema={:?}, filters={:?}",
            self.projected_schema, self.filters
        )
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

        // Register with DataFusion
        let ctx = SessionContext::new();
        ctx.register_table("zarr_table", Arc::new(provider))
            .unwrap();

        // Query the table
        let df = ctx.sql("SELECT * FROM zarr_table").await.unwrap();
        let batches = df.collect().await.unwrap();

        // Verify results
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        //assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    #[ignore = "Projection support"]
    async fn test_table_provider_with_sql() {
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();

        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();

        // Register with DataFusion
        let ctx = SessionContext::new();
        ctx.register_table("zarr_table", Arc::new(provider))
            .unwrap();

        // Query with projection and filter
        let df = ctx
            .sql("SELECT collection, date FROM zarr_table WHERE collection = 'collection_a'")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();

        // Verify results - DataFusion applies filter and projection
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 1);
        // Note: projection happens in DataFusion's optimizer, so we get only the requested columns
        assert_eq!(batch.num_columns(), 2);

        // Verify the collection value
        let collection_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        assert_eq!(collection_col.value(0), "collection_a");
    }

    /// Test that ST_Intersects correctly selects records that intersect with the query geometry.
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

        // Query with a polygon that intersects collection_a
        // The query box (0,0) to (5,5) is within collection_a's bbox (-10,-10) to (10,10)
        let sql = "
                SELECT collection FROM zarr_data
                WHERE ST_Intersects(
                    bbox,
                    ST_GeomFromText('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))')
                )
                ORDER BY collection
            ";

        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        // Should return at least collection_a (and possibly b, c since they also contain this area)
        assert!(!batches.is_empty(), "Query should return results");
        assert!(
            batches[0].num_rows() > 0,
            "Should have at least one matching row"
        );

        let collection_col = batches[0]
            .column_by_name("collection")
            .expect("collection column should exist");

        let collection_array = collection_col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("collection should be StringViewArray");

        let collections: Vec<&str> = (0..collection_array.len())
            .map(|i| collection_array.value(i))
            .collect();

        assert!(
            collections.contains(&"collection_a"),
            "collection_a should intersect with query box at (0,0) to (5,5)"
        );
    }

    /// Test that ST_Intersects correctly returns no records when query geometry doesn't intersect.
    #[tokio::test]
    async fn test_st_intersects_no_match() {
        let wrapper = get_local_zarr_store().await;
        let path = wrapper.get_store_path();

        let ctx = SessionContext::new();

        register_spatial_functions(&ctx).expect("Failed to register spatial functions");
        let provider = ZarrTableProvider::new_filesystem(path, "/meta").unwrap();

        ctx.register_table("zarr_data", Arc::new(provider))
            .expect("Failed to register table");

        // Query with a polygon that doesn't intersect any of the test data
        // The query box (100,100) to (110,110) is far from all test bboxes
        let sql = "
                SELECT collection FROM zarr_data
                WHERE ST_Intersects(
                    bbox,
                    ST_GeomFromText('POLYGON((100 100, 100 110, 110 110, 110 100, 100 100))')
                )
            ";

        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        assert!(batches.is_empty(), "Query should not return results");
    }

    /// Test that ST_Intersects works with a larger query box that intersects multiple records.
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

        // Query with a polygon that intersects collection_a and collection_b
        // The query box (-15,-15) to (15,15) overlaps with both a and b but possibly not c
        let sql = "
                SELECT collection FROM zarr_data
                WHERE ST_Intersects(
                    bbox,
                    ST_GeomFromText('POLYGON((-15 -15, -15 15, 15 15, 15 -15, -15 -15))')
                )
                ORDER BY collection
            ";

        let df = ctx.sql(sql).await.expect("Failed to execute query");
        let batches = df.collect().await.expect("Failed to collect results");

        // Should return multiple results
        assert!(!batches.is_empty(), "Query should return results");
        assert!(
            batches[0].num_rows() >= 2,
            "Should match at least collection_a and collection_b"
        );

        let collection_col = batches[0]
            .column_by_name("collection")
            .expect("collection column should exist");

        let collection_array = collection_col
            .as_any()
            .downcast_ref::<StringViewArray>()
            .expect("collection should be StringViewArray");

        let collections: Vec<&str> = (0..collection_array.len())
            .map(|i| collection_array.value(i))
            .collect();

        assert!(
            collections.contains(&"collection_a"),
            "collection_a should be in results"
        );
        assert!(
            collections.contains(&"collection_b"),
            "collection_b should be in results"
        );
    }
}
