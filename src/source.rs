use std::any::Any;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampSecondArray, BinaryArray};
use arrow_schema::{SchemaRef, DataType, TimeUnit};
use async_stream::stream;
use async_trait::async_trait;
use datafusion::common::Statistics;
use datafusion::datasource::physical_plan::{
    FileScanConfig, FileSource, FileOpener, FileMeta,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    metrics::ExecutionPlanMetricsSet, ExecutionPlan, SendableRecordBatchStream,
    DisplayAs, PlanProperties, Partitioning,
    execution_plan::{EmissionType, Boundedness},
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_expr::EquivalenceProperties;
use futures_util::StreamExt;
use object_store::ObjectStore;
use zarrs::group::Group;
use std::fmt;
use zarrs_filesystem::FilesystemStore;

#[derive(Debug, Clone)]
pub struct ZarrMetaSource {
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
    metrics: ExecutionPlanMetricsSet,
}

impl Default for ZarrMetaSource {
    fn default() -> Self {
        Self {
            batch_size: None,
            file_schema: None,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl ZarrMetaSource {
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            batch_size: None,
            file_schema: Some(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
    
    pub async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(conf.file_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        
        Ok(Arc::new(ZarrExec {
            base_config: conf,
            metrics: ExecutionPlanMetricsSet::new(),
            properties,
        }))
    }
}

/// Allows easy conversion from ParquetSource to Arc\<dyn FileSource\>;
impl From<ZarrMetaSource> for Arc<dyn FileSource> {
    fn from(source: ZarrMetaSource) -> Self {
        Arc::new(source)
    }
}

impl FileSource for ZarrMetaSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size: Some(batch_size),
            file_schema: self.file_schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            file_schema: Some(schema),
            batch_size: self.batch_size,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn with_projection(&self, _config: &FileScanConfig) -> Arc<dyn FileSource> {
        // For now, return self as projection is handled elsewhere
        Arc::new(self.clone())
    }

    fn with_statistics(&self, _statistics: Statistics) -> Arc<dyn FileSource> {
        // For now, return self as statistics are handled elsewhere
        Arc::new(self.clone())
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    fn file_type(&self) -> &str {
        "zarr"
    }

    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        _partition: usize,
    ) -> Arc<dyn FileOpener> {
        Arc::new(ZarrFileOpener {
            schema: base_config.file_schema.clone(),
        })
    }
}

#[derive(Debug)]
struct ZarrExec {
    base_config: FileScanConfig,
    metrics: ExecutionPlanMetricsSet,
    properties: PlanProperties,
}

impl DisplayAs for ZarrExec {
    fn fmt_as(&self, _t: datafusion::physical_plan::DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ZarrExec")
    }
}

#[async_trait]
impl ExecutionPlan for ZarrExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.base_config.file_schema.clone()
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
        let schema = self.base_config.file_schema.clone();
        let file_meta = &self.base_config.file_groups[0][0];
        let zarr_path = file_meta.object_meta.location.to_string();
        
        let zarr_dir = if zarr_path.ends_with("/zarr.json") {
            zarr_path[..zarr_path.len() - 10].to_string() // Remove "/zarr.json"
        } else {
            zarr_path
        };

        let stream = stream! {
            match read_zarr_data(&zarr_dir, schema.clone()).await {
                Ok(batch) => yield Ok(batch),
                Err(e) => yield Err(e),
            }
        };

        let schema_clone = schema.clone();
        let stream = stream! {
            match read_zarr_data(&zarr_dir, schema_clone.clone()).await {
                Ok(batch) => yield Ok(batch),
                Err(e) => yield Err(e),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn name(&self) -> &str {
        "ZarrExec"
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[derive(Debug)]
struct ZarrFileOpener {
    schema: SchemaRef,
}

#[async_trait]
impl FileOpener for ZarrFileOpener {
    async fn open(&self, file_meta: FileMeta, _partition: &PartitionedFile) -> Result<SendableRecordBatchStream> {
        let zarr_path = file_meta.object_meta.location.to_string();
        
        let zarr_dir = if zarr_path.ends_with("/zarr.json") {
            zarr_path[..zarr_path.len() - 10].to_string() // Remove "/zarr.json"
        } else {
            zarr_path
        };

        let schema = self.schema.clone();
        let stream = stream! {
            match read_zarr_data(&zarr_dir, schema.clone()).await {
                Ok(batch) => yield Ok(batch),
                Err(e) => yield Err(e),
            }
        };

        let schema_clone = schema.clone();
        let stream = stream! {
            match read_zarr_data(&zarr_dir, schema_clone.clone()).await {
                Ok(batch) => yield Ok(batch),
                Err(e) => yield Err(e),
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

async fn read_zarr_data(zarr_dir: &str, schema: SchemaRef) -> Result<RecordBatch> {
    let store = FilesystemStore::new(zarr_dir)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    
    let group = Group::open(Arc::new(store), "/")
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut columns: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let field_name = field.name();
        
        let array_reader = group.array(field_name)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        let zarr_array = array_reader.retrieve_array_subset_elements::<Vec<u8>>(&[..])
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        let arrow_array: ArrayRef = match field.data_type() {
            DataType::Utf8 => {
                // Read as strings
                let string_array = array_reader.retrieve_array_subset_elements::<String>(&[..])
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Arc::new(StringArray::from(string_array.into_raw_vec().unwrap()))
            },
            DataType::Binary => {
                // For variable length bytes, we'll read as binary
                let bytes_vec: Vec<Option<Vec<u8>>> = zarr_array.into_raw_vec().unwrap().into_iter().map(|b| Some(b)).collect();
                Arc::new(BinaryArray::from_opt_vec(bytes_vec))
            },
            DataType::Timestamp(TimeUnit::Second, None) => {
                // Read as datetime64 seconds
                let datetime_array = array_reader.retrieve_array_subset_elements::<i64>(&[..])
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Arc::new(TimestampSecondArray::from(datetime_array.into_raw_vec().unwrap()))
            },
            _ => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type: {}", field.data_type()
                )));
            }
        };
        
        columns.push(arrow_array);
    }
    
    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}
