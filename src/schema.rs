//! Generate an Arrow schema from a Zarr array schema.

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use geoarrow_schema::{Crs, WkbType};
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::data_type::DataType as ZarrDataType;
use zarrs::array::FillValue;
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
            ZarrDataType::Bytes => {
                let crs = Crs::from_authority_code("EPSG:4326".to_string());
                let geoarrow_metadata = Arc::new(geoarrow_schema::Metadata::new(crs, None));

                return Ok(Arc::new(
                    Field::new(&name, DataType::BinaryView, false)
                        .with_extension_type(WkbType::new(geoarrow_metadata)),
                ));
            }
            _ => {
                return Err(ZarrDataFusionError::Custom(format!(
                    "Expected 'bbox' field to be of Zarr Bytes data type, got: {:?}",
                    zarr_dtype
                )));
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
        ZarrDataType::Bytes => DataType::BinaryView,
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

/// Map an Arrow DataType to a Zarr DataType for writing.
/// Returns None for non-scalar types that cannot be stored as 1D Zarr arrays.
pub(crate) fn arrow_to_zarr_dtype(arrow_type: &DataType) -> Option<ZarrDataType> {
    match arrow_type {
        DataType::Boolean => Some(ZarrDataType::Bool),
        DataType::Int8 => Some(ZarrDataType::Int8),
        DataType::Int16 => Some(ZarrDataType::Int16),
        DataType::Int32 => Some(ZarrDataType::Int32),
        DataType::Int64 => Some(ZarrDataType::Int64),
        DataType::UInt8 => Some(ZarrDataType::UInt8),
        DataType::UInt16 => Some(ZarrDataType::UInt16),
        DataType::UInt32 => Some(ZarrDataType::UInt32),
        DataType::UInt64 => Some(ZarrDataType::UInt64),
        DataType::Float32 => Some(ZarrDataType::Float32),
        DataType::Float64 => Some(ZarrDataType::Float64),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Some(ZarrDataType::String)
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Some(ZarrDataType::Bytes)
        }
        DataType::Timestamp(TimeUnit::Second, _) => Some(ZarrDataType::NumpyDateTime64 {
            unit: NumpyTimeUnit::Second,
            scale_factor: std::num::NonZeroU32::new(1).unwrap(),
        }),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(ZarrDataType::NumpyDateTime64 {
            unit: NumpyTimeUnit::Millisecond,
            scale_factor: std::num::NonZeroU32::new(1).unwrap(),
        }),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(ZarrDataType::NumpyDateTime64 {
            unit: NumpyTimeUnit::Microsecond,
            scale_factor: std::num::NonZeroU32::new(1).unwrap(),
        }),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(ZarrDataType::NumpyDateTime64 {
            unit: NumpyTimeUnit::Nanosecond,
            scale_factor: std::num::NonZeroU32::new(1).unwrap(),
        }),
        _ => None,
    }
}

/// Return the default Zarr fill value for a given Zarr DataType.
pub(crate) fn zarr_fill_value(dtype: &ZarrDataType) -> FillValue {
    match dtype {
        ZarrDataType::Bool => FillValue::from(false),
        ZarrDataType::Int8 => FillValue::from(0i8),
        ZarrDataType::Int16 => FillValue::from(0i16),
        ZarrDataType::Int32 => FillValue::from(0i32),
        ZarrDataType::Int64 => FillValue::from(0i64),
        ZarrDataType::UInt8 => FillValue::from(0u8),
        ZarrDataType::UInt16 => FillValue::from(0u16),
        ZarrDataType::UInt32 => FillValue::from(0u32),
        ZarrDataType::UInt64 => FillValue::from(0u64),
        ZarrDataType::Float32 => FillValue::from(0.0f32),
        ZarrDataType::Float64 => FillValue::from(0.0f64),
        ZarrDataType::String => FillValue::from(""),
        ZarrDataType::Bytes => FillValue::from(vec![]),
        ZarrDataType::NumpyDateTime64 { .. } => FillValue::from(0i64),
        _ => FillValue::from(0u8),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::testing::utils::get_local_zarr_store;
    use zarrs_filesystem::FilesystemStore;

    #[tokio::test]
    async fn test_schema_from_zarr_group() {
        let wrapper = get_local_zarr_store(false).await;
        let path = wrapper.get_store_path();

        {
            let storage = Arc::new(FilesystemStore::new(path).unwrap());

            let group = Group::open(storage.clone(), "/meta").unwrap();
            let schema = group_arrays_schema(&group).unwrap();

            let geoarrow_metadata = Arc::new(geoarrow_schema::Metadata::new(
                Crs::from_authority_code("EPSG:4326".to_string()),
                None,
            ));

            let expected_fields = vec![
                Arc::new(
                    Field::new("bbox", DataType::BinaryView, false)
                        .with_extension_type(WkbType::new(geoarrow_metadata)),
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

    #[test]
    fn test_arrow_to_zarr_dtype_scalars() {
        use arrow_schema::TimeUnit;

        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Boolean),
            Some(ZarrDataType::Bool)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Int64),
            Some(ZarrDataType::Int64)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::UInt32),
            Some(ZarrDataType::UInt32)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Float64),
            Some(ZarrDataType::Float64)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Utf8),
            Some(ZarrDataType::String)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::LargeUtf8),
            Some(ZarrDataType::String)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Utf8View),
            Some(ZarrDataType::String)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Binary),
            Some(ZarrDataType::Bytes)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::BinaryView),
            Some(ZarrDataType::Bytes)
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Timestamp(TimeUnit::Millisecond, None)),
            Some(ZarrDataType::NumpyDateTime64 { unit: NumpyTimeUnit::Millisecond, .. })
        ));
        assert!(matches!(
            arrow_to_zarr_dtype(&DataType::Timestamp(TimeUnit::Second, None)),
            Some(ZarrDataType::NumpyDateTime64 { unit: NumpyTimeUnit::Second, .. })
        ));
    }

    #[test]
    fn test_arrow_to_zarr_dtype_non_scalar_returns_none() {
        use arrow_schema::Field;
        assert!(arrow_to_zarr_dtype(&DataType::List(Arc::new(
            Field::new("item", DataType::Int64, true)
        )))
        .is_none());
        assert!(arrow_to_zarr_dtype(&DataType::Struct(arrow_schema::Fields::empty())).is_none());
    }

    #[test]
    fn test_zarr_fill_value_types() {
        // Just verify it returns without panic for supported types
        let _ = zarr_fill_value(&ZarrDataType::Int64);
        let _ = zarr_fill_value(&ZarrDataType::String);
        let _ = zarr_fill_value(&ZarrDataType::Bytes);
        let _ = zarr_fill_value(&ZarrDataType::NumpyDateTime64 {
            unit: NumpyTimeUnit::Millisecond,
            scale_factor: std::num::NonZeroU32::new(1).unwrap(),
        });
    }
}
