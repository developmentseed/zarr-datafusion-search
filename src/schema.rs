//! Generate an Arrow schema from a Zarr array schema.

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use geoarrow_schema::{Crs, WktType};
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::data_type::DataType as ZarrDataType;
use zarrs::group::Group;
use zarrs::metadata_ext::data_type::NumpyTimeUnit;
use zarrs::node::NodePath;
use zarrs::storage::{AsyncReadableListableStorageTraits, ReadableListableStorageTraits};

use crate::error::{ZarrDataFusionError, ZarrDataFusionResult};

/// Infer an Arrow schema from the arrays in a Zarr group
pub fn group_arrays_schema<TStorage: ?Sized + ReadableListableStorageTraits>(
    group: &Group<TStorage>,
) -> ZarrDataFusionResult<SchemaRef> {
    arrays_to_schema(group.path(), &group.child_arrays()?)
}

/// Infer an Arrow schema from the arrays in a Zarr group asynchronously
pub async fn group_arrays_schema_async<TStorage: ?Sized + AsyncReadableListableStorageTraits>(
    group: &Group<TStorage>,
) -> ZarrDataFusionResult<SchemaRef> {
    arrays_to_schema(group.path(), &group.async_child_arrays().await?)
}

fn arrays_to_schema<TStorage: ?Sized>(
    group_root: &NodePath,
    arrays: &[Array<TStorage>],
) -> ZarrDataFusionResult<SchemaRef> {
    let mut fields = vec![];
    for array in arrays.iter() {
        let name = field_name(group_root, array.path());
        fields.push(zarr_to_arrow_field(name, array.data_type())?);
    }
    // Sort fields by name for consistent ordering
    fields.sort_by(|f1, f2| f1.name().cmp(f2.name()));
    Ok(Arc::new(Schema::new(fields)))
}

fn field_name(group_root: &NodePath, array_path: &NodePath) -> String {
    assert!(array_path.as_str().starts_with(group_root.as_str()),);
    // Converts from /meta/collection to /collection
    let array_name_with_slash = array_path
        .as_str()
        .strip_prefix(group_root.as_str())
        .expect("Array path must be within the group root");

    // Converts from /collection to collection
    array_name_with_slash.trim_start_matches('/').to_string()
}

/// Maps a Zarr data type to an Arrow data type
fn zarr_to_arrow_field(name: String, zarr_dtype: &ZarrDataType) -> ZarrDataFusionResult<FieldRef> {
    if name == "bbox" {
        match zarr_dtype {
            ZarrDataType::String => {
                let crs = Crs::from_authority_code("EPSG:4326".to_string());
                let geoarrow_metadata = Arc::new(geoarrow_schema::Metadata::new(crs, None));

                return Ok(Arc::new(
                    Field::new(&name, DataType::Utf8View, false)
                        .with_extension_type(WktType::new(geoarrow_metadata)),
                ));
            }
            _ => {
                return Err(ZarrDataFusionError::Custom(
                    "Expected 'bbox' field to be of Zarr string data type.".to_string(),
                ));
            }
        }
    }

    let data_type = match zarr_dtype {
        ZarrDataType::Bool => DataType::Boolean,
        ZarrDataType::Int8 => DataType::Int8,
        ZarrDataType::Int16 => DataType::Int16,
        ZarrDataType::Int32 => DataType::Int32,
        ZarrDataType::Int64 => DataType::Int64,
        ZarrDataType::UInt8 => DataType::UInt8,
        ZarrDataType::UInt16 => DataType::UInt16,
        ZarrDataType::UInt32 => DataType::UInt32,
        ZarrDataType::UInt64 => DataType::UInt64,
        ZarrDataType::Float16 => DataType::Float16,
        ZarrDataType::Float32 => DataType::Float32,
        ZarrDataType::Float64 => DataType::Float64,
        ZarrDataType::Complex64 | ZarrDataType::Complex128 => {
            return Err(ZarrDataFusionError::Custom(
                "Complex64/Complex128 not yet supported.".to_string(),
            ));
        }
        ZarrDataType::RawBits(_size) => DataType::BinaryView,
        ZarrDataType::String => DataType::Utf8View,
        ZarrDataType::NumpyDateTime64 {
            unit,
            scale_factor: _,
        } => match unit {
            NumpyTimeUnit::Millisecond => DataType::Timestamp(TimeUnit::Millisecond, None),
            NumpyTimeUnit::Microsecond => DataType::Timestamp(TimeUnit::Microsecond, None),
            NumpyTimeUnit::Nanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),
            NumpyTimeUnit::Second => DataType::Timestamp(TimeUnit::Second, None),
            _ => {
                return Err(ZarrDataFusionError::Custom(format!(
                    "Unsupported Numpy datetime64 time unit: {:?}",
                    unit
                )));
            }
        },
        ZarrDataType::Extension(ext) => {
            return Err(ZarrDataFusionError::Custom(format!(
                "Unsupported Zarr extension type: {}",
                ext.name()
            )));
        }
        _ => {
            return Err(ZarrDataFusionError::Custom(format!(
                "Unsupported Zarr data type: {:?}",
                zarr_dtype
            )));
        }
    };
    Ok(Arc::new(Field::new(&name, data_type, false)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use zarrs_filesystem::FilesystemStore;

    #[test]
    fn test_schema_from_zarr_group() {
        let storage = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

        let group = Group::open(storage.clone(), "/meta").unwrap();
        let schema = group_arrays_schema(&group).unwrap();

        let geoarrow_metadata = Arc::new(geoarrow_schema::Metadata::new(
            Crs::from_authority_code("EPSG:4326".to_string()),
            None,
        ));

        let expected_fields = vec![
            Arc::new(
                Field::new("bbox", DataType::Utf8View, false)
                    .with_extension_type(WktType::new(geoarrow_metadata)),
            ),
            Arc::new(Field::new("collection", DataType::Utf8View, false)),
            Arc::new(Field::new(
                "date",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            )),
        ];
        let expected_schema = Arc::new(Schema::new(expected_fields));
        assert_eq!(&schema, &expected_schema);
    }
}
