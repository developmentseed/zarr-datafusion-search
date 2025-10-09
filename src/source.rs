use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Statistics;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

#[derive(Debug, Clone)]
pub struct ZarrMetaSource {
    batch_size: Option<usize>,
    file_schema: Option<SchemaRef>,
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
            ..self.clone()
        })
    }

    fn with_schema(&self, schema: SchemaRef) -> Arc<dyn FileSource> {
        Arc::new(Self {
            file_schema: Some(schema),
            ..self.clone()
        })
    }

    fn with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource> {
        todo!()
    }

    fn with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource> {
        todo!()
    }

    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        todo!()
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    fn file_type(&self) -> &str {
        "zarr"
    }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn object_store::ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Arc<dyn datafusion::datasource::physical_plan::FileOpener> {
        todo!()
    }
}
