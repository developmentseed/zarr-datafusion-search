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

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ZarrExec::new(
            self.zarr_backend.clone(),
            self.schema.clone(),
            projection.cloned(),
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

/// Custom ExecutionPlan that loads data from Zarr on execution
#[derive(Debug)]
struct ZarrExec {
    zarr_backend: ZarrBackend,
    schema: SchemaRef,
    #[allow(dead_code)]
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl ZarrExec {
    fn new(zarr_backend: ZarrBackend, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            zarr_backend,
            schema,
            projection,
            properties,
        }
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
        self.schema.clone()
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
        let stream_schema = self.schema.clone();
        let stream = RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(
                async move { Ok(backend.load_record_batch(stream_schema).await?) },
            ),
        );
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for ZarrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZarrExec: backend={:?}", self.zarr_backend)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_basic_table_provider() {
        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr", "/meta").unwrap();

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
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
    }

    #[tokio::test]
    #[ignore = "Projection support"]
    async fn test_table_provider_with_sql() {
        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr", "/meta").unwrap();

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

        let ctx = SessionContext::new();

        register_spatial_functions(&ctx).expect("Failed to register spatial functions");

        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr", "/meta")
            .expect("Failed to create table provider");
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
        let ctx = SessionContext::new();

        register_spatial_functions(&ctx).expect("Failed to register spatial functions");

        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr", "/meta")
            .expect("Failed to create table provider");
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

        let ctx = SessionContext::new();

        register_spatial_functions(&ctx).expect("Failed to register spatial functions");

        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr", "/meta")
            .expect("Failed to create table provider");
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
