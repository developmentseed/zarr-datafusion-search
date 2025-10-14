use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
use geoarrow_array::array::WktArray;
use geoarrow_schema::{Crs, WktType};
use object_store::ObjectStore;
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;
use zarrs::array::{Array, ElementOwned};
use zarrs::array_subset::ArraySubset;
use zarrs::storage::{AsyncReadableListableStorageTraits, ReadableListableStorageTraits};
use zarrs_filesystem::{FilesystemStore, FilesystemStoreCreateError};
use zarrs_storage::{MaybeSend, MaybeSync};

use crate::error::ZarrDataFusionResult;

/// A simple DataFusion table provider that loads data from a Zarr store
#[derive(Debug)]
pub struct ZarrTableProvider {
    schema: SchemaRef,
    zarr_backend: ZarrBackend,
}

impl ZarrTableProvider {
    /// Create a new ZarrTableProvider from a Zarr store path
    pub fn new_filesystem<P: AsRef<std::path::Path>>(
        zarr_path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let zarr_backend = ZarrBackend::new_filesystem(zarr_path)?;
        let schema = Self::construct_schema();
        Ok(Self {
            schema,
            zarr_backend,
        })
    }

    pub fn new_object_store<T: ObjectStore>(store: T) -> Self {
        let zarr_backend = ZarrBackend::new_object_store(store);
        let schema = Self::construct_schema();
        Self {
            schema,
            zarr_backend,
        }
    }

    fn construct_schema() -> SchemaRef {
        // Define the schema based on the expected Zarr arrays
        let wkt_crs = Crs::from_authority_code("EPSG:4326".to_string());
        let wkt_metadata = Arc::new(geoarrow_schema::Metadata::new(wkt_crs, None));

        Arc::new(Schema::new(vec![
            Field::new("collection", DataType::Utf8, false),
            Field::new(
                "date",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("wkt_field", DataType::Utf8, false)
                .with_extension_type(WktType::new(wkt_metadata)),
        ]))
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
    fn load_array<T: ElementOwned>(&self, path: &str) -> ZarrDataFusionResult<Vec<T>> {
        let array = Array::open(self.0.clone(), path)?;
        let full_subset = ArraySubset::new_with_shape(array.shape().to_vec());
        Ok(array.retrieve_array_subset_elements(&full_subset)?)
    }
}

#[derive(Clone)]
struct AsyncZarrBackend(Arc<dyn AsyncReadableListableStorageTraits>);

impl AsyncZarrBackend {
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
}

#[derive(Clone)]
enum ZarrBackend {
    Sync(SyncZarrBackend),
    Async(AsyncZarrBackend),
}

impl Debug for ZarrBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ZarrBackend::Sync(_) => write!(f, "ZarrBackend::Sync"),
            ZarrBackend::Async(_) => write!(f, "ZarrBackend::Async"),
        }
    }
}

impl ZarrBackend {
    fn new_filesystem<P: AsRef<std::path::Path>>(
        base_path: P,
    ) -> Result<Self, FilesystemStoreCreateError> {
        Ok(Self::Sync(SyncZarrBackend(Arc::new(FilesystemStore::new(
            base_path,
        )?))))
    }

    fn new_object_store<T: ObjectStore>(store: T) -> Self {
        Self::Async(AsyncZarrBackend(Arc::new(
            zarrs_object_store::AsyncObjectStore::new(store),
        )))
    }

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
        let collection_arrow: ArrayRef = Arc::new(StringArray::from(collection_data));
        let date_arrow: ArrayRef = Arc::new(TimestampMillisecondArray::from(date_data));
        let wkt_crs = Crs::from_authority_code("EPSG:4326".to_string());
        let wkt_metadata = Arc::new(geoarrow_schema::Metadata::new(wkt_crs, None));
        let wkt_arrow = WktArray::new(bbox_data.into(), wkt_metadata);

        // Create the RecordBatch
        let record_batch = RecordBatch::try_new(
            schema,
            vec![collection_arrow, date_arrow, wkt_arrow.into_array_ref()],
        )?;

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
        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr").unwrap();

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
        let provider = ZarrTableProvider::new_filesystem("data/zarr_store.zarr").unwrap();

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
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(collection_col.value(0), "collection_a");
    }
}
