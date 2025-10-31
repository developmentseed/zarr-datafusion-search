use arrow_array::{ArrayRef, RecordBatch, StringViewArray, TimestampMillisecondArray};
use arrow_schema::SchemaRef;
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
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::WktViewArray;
use geoarrow_schema::Crs;
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;
use zarrs::array::{Array, ElementOwned};
use zarrs::array_subset::ArraySubset;
use zarrs::group::Group;
use zarrs::storage::{AsyncReadableListableStorageTraits, ReadableListableStorageTraits};
use zarrs_filesystem::{FilesystemStore, FilesystemStoreCreateError};
use zarrs_storage::{MaybeSend, MaybeSync};

use crate::error::ZarrDataFusionResult;
use crate::schema::{group_arrays_schema, group_arrays_schema_async};

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
        group_path: &str,
    ) -> ZarrDataFusionResult<Self> {
        let zarr_backend = AsyncZarrBackend::new_icechunk(icechunk_session);
        let schema = zarr_backend.infer_group_schema(group_path).await?;
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
struct AsyncZarrBackend(Arc<dyn AsyncReadableListableStorageTraits>);

impl AsyncZarrBackend {
    fn new_object_store<T: ObjectStore>(store: T) -> Self {
        AsyncZarrBackend(Arc::new(zarrs_object_store::AsyncObjectStore::new(store)))
    }

    fn new_icechunk(icechunk_session: icechunk::session::Session) -> Self {
        AsyncZarrBackend(Arc::new(zarrs_icechunk::AsyncIcechunkStore::new(
            icechunk_session,
        )))
    }

    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync>(
        &self,
        path: &str,
    ) -> ZarrDataFusionResult<Vec<T>> {
        let array = Array::async_open(self.0.clone(), path).await?;
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
    Sync(SyncZarrBackend),
}

impl Debug for ZarrBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZarrBackend::Async(_) => write!(f, "ZarrBackend::Async"),
            ZarrBackend::Sync(_) => write!(f, "ZarrBackend::Sync"),
        }
    }
}

impl From<AsyncZarrBackend> for ZarrBackend {
    fn from(async_backend: AsyncZarrBackend) -> Self {
        ZarrBackend::Async(async_backend)
    }
}

impl From<SyncZarrBackend> for ZarrBackend {
    fn from(sync_backend: SyncZarrBackend) -> Self {
        ZarrBackend::Sync(sync_backend)
    }
}

impl ZarrBackend {
    // fn new_filesystem<P: AsRef<std::path::Path>>(
    //     base_path: P,
    // ) -> Result<Self, FilesystemStoreCreateError> {
    //     Ok(Self::Sync(SyncZarrBackend::new_filesystem(base_path)?))
    // }

    // fn new_object_store<T: ObjectStore>(store: T) -> Self {
    //     Self::Async(AsyncZarrBackend(Arc::new(
    //         zarrs_object_store::AsyncObjectStore::new(store),
    //     )))
    // }

    async fn load_array<T: ElementOwned + MaybeSend + MaybeSync>(
        &self,
        path: &str,
    ) -> ZarrDataFusionResult<Vec<T>> {
        match self {
            ZarrBackend::Sync(sync_backend) => sync_backend.load_array(path),
            ZarrBackend::Async(async_backend) => async_backend.load_array(path).await,
        }
    }

    async fn load_record_batch(self, schema: SchemaRef) -> ZarrDataFusionResult<RecordBatch> {
        let collection_data: Vec<String> = self.load_array("/meta/collection").await?;
        let date_data: Vec<i64> = self.load_array("/meta/date").await?;
        let bbox_data: Vec<String> = self.load_array("/meta/bbox").await?;

        // Create Arrow arrays from the loaded data
        let collection_arrow: ArrayRef = Arc::new(StringViewArray::from(collection_data));
        let date_arrow: ArrayRef = Arc::new(TimestampMillisecondArray::from(date_data));
        let wkt_crs = Crs::from_authority_code("EPSG:4326".to_string());
        let wkt_metadata = Arc::new(geoarrow_schema::Metadata::new(wkt_crs, None));
        let wkt_arrow = WktViewArray::new(bbox_data.into(), wkt_metadata);

        let columns = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "collection" => collection_arrow.clone(),
                "date" => date_arrow.clone(),
                "bbox" => wkt_arrow.clone().into_array_ref(),
                _ => panic!("Unexpected field name: {}", field.name()),
            })
            .collect();

        // Create the RecordBatch
        let record_batch = RecordBatch::try_new(schema, columns)?;

        Ok(record_batch)
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
}
