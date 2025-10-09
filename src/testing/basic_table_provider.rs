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
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs_filesystem::FilesystemStore;

use crate::error::ZarrDataFusionResult;

/// A simple DataFusion table provider that loads data from a Zarr store
#[derive(Debug)]
pub struct BasicZarrTableProvider {
    schema: SchemaRef,
    zarr_path: String,
}

impl BasicZarrTableProvider {
    /// Create a new BasicZarrTableProvider from a Zarr store path
    pub fn new(zarr_path: String) -> Result<Self, Box<dyn std::error::Error>> {
        // Define the schema based on the expected Zarr arrays
        let wkt_crs = Crs::from_authority_code("EPSG:4326".to_string());
        let wkt_metadata = Arc::new(geoarrow_schema::Metadata::new(wkt_crs, None));

        let schema = Arc::new(Schema::new(vec![
            Field::new("collection", DataType::Utf8, false),
            Field::new(
                "date",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("wkt_field", DataType::Utf8, false)
                .with_extension_type(WktType::new(wkt_metadata)),
        ]));

        Ok(Self { schema, zarr_path })
    }
}

#[async_trait]
impl TableProvider for BasicZarrTableProvider {
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
            self.zarr_path.clone(),
            self.schema.clone(),
            projection.cloned(),
        )))
    }
}

/// Custom ExecutionPlan that loads data from Zarr on execution
#[derive(Debug)]
struct ZarrExec {
    zarr_path: String,
    schema: SchemaRef,
    #[allow(dead_code)]
    projection: Option<Vec<usize>>,
    properties: PlanProperties,
}

impl ZarrExec {
    fn new(zarr_path: String, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            zarr_path,
            schema,
            projection,
            properties,
        }
    }

    fn load_data(&self) -> ZarrDataFusionResult<RecordBatch> {
        // Create a filesystem store pointing to the zarr store
        let store = Arc::new(FilesystemStore::new(&self.zarr_path)?);

        // Load collection array (strings)
        let collection_array = Array::open(store.clone(), "/meta/collection")?;
        let collection_subset = ArraySubset::new_with_shape(collection_array.shape().to_vec());
        let collection_data: Vec<String> =
            collection_array.retrieve_array_subset_elements(&collection_subset)?;

        // Load date array (datetime64[ms])
        let date_array = Array::open(store.clone(), "/meta/date")?;
        let date_subset = ArraySubset::new_with_shape(date_array.shape().to_vec());
        let date_data: Vec<i64> = date_array.retrieve_array_subset_elements(&date_subset)?;

        // Load bbox array (strings representing WKT geometries)
        let bbox_array = Array::open(store.clone(), "/meta/bbox")?;
        let bbox_subset = ArraySubset::new_with_shape(bbox_array.shape().to_vec());
        let bbox_data: Vec<String> = bbox_array.retrieve_array_subset_elements(&bbox_subset)?;

        // Create Arrow arrays from the loaded data
        let collection_arrow: ArrayRef = Arc::new(StringArray::from(collection_data));
        let date_arrow: ArrayRef = Arc::new(TimestampMillisecondArray::from(date_data));
        let wkt_crs = Crs::from_authority_code("EPSG:4326".to_string());
        let wkt_metadata = Arc::new(geoarrow_schema::Metadata::new(wkt_crs, None));
        let wkt_arrow = WktArray::new(bbox_data.into(), wkt_metadata);

        // Create the RecordBatch
        let record_batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![collection_arrow, date_arrow, wkt_arrow.into_array_ref()],
        )?;

        Ok(record_batch)
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
        let record_batch = self.load_data()?;
        let schema = record_batch.schema();
        let stream = RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async move { Ok(record_batch) }),
        );
        Ok(Box::pin(stream))
    }
}

impl DisplayAs for ZarrExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZarrExec: path={}", self.zarr_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_basic_table_provider() {
        let provider = BasicZarrTableProvider::new("data/zarr_store.zarr".to_string()).unwrap();

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
        let provider = BasicZarrTableProvider::new("data/zarr_store.zarr".to_string()).unwrap();

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
