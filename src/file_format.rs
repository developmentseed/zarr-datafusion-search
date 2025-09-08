use std::any::Any;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use object_store::{ObjectMeta, ObjectStore};
use zarrs::group::Group;
use zarrs_filesystem::FilesystemStore;

use crate::source::ZarrMetaSource;

#[derive(Debug, Clone, Default)]
pub struct ZarrMetaFormatFactory {}

impl FileFormatFactory for ZarrMetaFormatFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
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
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        if objects.is_empty() {
            return Err(DataFusionError::Internal("No objects provided".to_string()));
        }

        let zarr_path = &objects[0].location;
        let path_str = zarr_path.to_string();

        // Remove the trailing zarr.json if present to get the zarr directory
        let zarr_dir = if path_str.ends_with("/zarr.json") {
            &path_str[..path_str.len() - 10] // Remove "/zarr.json"
        } else {
            &path_str
        };

        let store =
            FilesystemStore::new(zarr_dir).map_err(|e| DataFusionError::External(Box::new(e)))?;

        let group = Group::open(Arc::new(store), "/")
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let children = group
            .children(false)
            .unwrap()
            .into_iter()
            .filter_map(|node| match node {
                zarrs::storage::Node::Array(_, name) => Some(name),
                _ => None,
            })
            .collect::<Vec<_>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut fields = Vec::new();

        for child_key in children {
            // For now, create a simple schema based on the array name
            let arrow_type = match child_key.as_str() {
                "bbox" => DataType::Binary,
                "collection" => DataType::Utf8,
                "date" => DataType::Timestamp(TimeUnit::Second, None),
                _ => DataType::Utf8,
            };
            // .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let zarr_data_type = array_reader.data_type();
            let arrow_type = match zarr_data_type {
                zarrs::array::DataType::String => DataType::Utf8,
                zarrs::array::DataType::RawBits(_) => DataType::Binary,
                _ => {
                    // Check if it's a datetime type by examining metadata
                    if let Some(attributes) = array_reader.attributes() {
                        if attributes.contains_key("_ARRAY_DIMENSIONS") {
                            DataType::Timestamp(TimeUnit::Second, None)
                        } else {
                            DataType::Utf8 // Default to string for unknown types
                        }
                    } else {
                        DataType::Utf8 // Default to string for unknown types
                    }
                }
            };

            fields.push(Field::new(child_key, arrow_type, true));
        }

        let schema = Schema::new(fields);
        Ok(Arc::new(schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        // For now, return default statistics
        // Could be enhanced to read actual statistics from Zarr metadata
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let source = ZarrMetaSource::new(conf.file_schema.clone());
        source.create_physical_plan(conf).await
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
        Arc::new(ZarrMetaSource::default())
    }
}
