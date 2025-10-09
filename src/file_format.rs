use std::any::Any;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::error::Result;
use datafusion::physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};

#[derive(Debug, Clone, Default)]
pub struct ZarrMetaFormatFactory {}

impl FileFormatFactory for ZarrMetaFormatFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        state: &dyn Session,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(self.default())
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(ZarrMetaFormat::default())
    }
}

impl GetExt for ZarrMetaFormatFactory {
    fn get_ext(&self) -> String {
        ".zarr".to_string()
    }
}

#[derive(Debug, Default)]
pub struct ZarrMetaFormat {}

#[async_trait]
impl FileFormat for ZarrMetaFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        ".zarr".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        todo!()
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        dbg!("infer_schema called");
        todo!()
        // let schema = self.inner.infer_schema(state, store, objects).await?;
        // // Insert GeoArrow metadata onto geometry column
        // if let Some(geo_meta_str) = schema.metadata().get("geo") {
        //     let geo_meta: GeoParquetMetadata = serde_json::from_str(geo_meta_str).unwrap();
        //     let new_schema =
        //         infer_geoarrow_schema(&schema, &geo_meta, self.parse_to_native, self.coord_type)
        //             .unwrap();
        //     Ok(new_schema)
        // } else {
        //     Ok(schema)
        // }
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        todo!()
        // self.inner
        //     .infer_stats(_state, store, table_schema, object)
        //     .await
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
        // self.inner.create_physical_plan(_state, conf).await
    }

    // async fn create_writer_physical_plan(
    //     &self,
    //     _input: Arc<dyn ExecutionPlan>,
    //     _state: &dyn Session,
    //     _conf: FileSinkConfig,
    //     _order_requirements: Option<LexRequirement>,
    // ) -> Result<Arc<dyn ExecutionPlan>> {
    //     todo!("writing not implemented for GeoParquet yet")
    // }

    fn file_source(&self) -> Arc<dyn FileSource> {
        todo!()
        // Arc::new(GeoParquetSource::default())
    }
}
