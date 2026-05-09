//! Generate an Arrow schema from a Zarr array schema.

use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use geoarrow_schema::{Crs, WkbType};
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::DataType as ZarrDataType;
use zarrs::array::FillValue;
use zarrs::array::data_type::{
    self, BoolDataType, BytesDataType, Float16DataType, Float32DataType, Float64DataType,
    Int8DataType, Int16DataType, Int32DataType, Int64DataType, NumpyDateTime64DataType,
    NumpyTimeUnit, RawBitsDataType, StringDataType, UInt8DataType, UInt16DataType, UInt32DataType,
    UInt64DataType,
};
use zarrs::group::Group;
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
        if zarr_dtype.is::<BytesDataType>() {
            let crs = Crs::from_authority_code("EPSG:4326".to_string());
            let geoarrow_metadata = Arc::new(geoarrow_schema::Metadata::new(crs, None));

            return Ok(Arc::new(
                Field::new(&name, DataType::BinaryView, false)
                    .with_extension_type(WkbType::new(geoarrow_metadata)),
            ));
        } else {
            return Err(ZarrDataFusionError::Custom(format!(
                "Expected 'bbox' field to be of Zarr Bytes data type, got: {:?}",
                zarr_dtype
            )));
        }
    }

    let data_type = if zarr_dtype.is::<BoolDataType>() {
        DataType::Boolean
    } else if zarr_dtype.is::<Int8DataType>() {
        DataType::Int8
    } else if zarr_dtype.is::<Int16DataType>() {
        DataType::Int16
    } else if zarr_dtype.is::<Int32DataType>() {
        DataType::Int32
    } else if zarr_dtype.is::<Int64DataType>() {
        DataType::Int64
    } else if zarr_dtype.is::<UInt8DataType>() {
        DataType::UInt8
    } else if zarr_dtype.is::<UInt16DataType>() {
        DataType::UInt16
    } else if zarr_dtype.is::<UInt32DataType>() {
        DataType::UInt32
    } else if zarr_dtype.is::<UInt64DataType>() {
        DataType::UInt64
    } else if zarr_dtype.is::<Float16DataType>() {
        DataType::Float16
    } else if zarr_dtype.is::<Float32DataType>() {
        DataType::Float32
    } else if zarr_dtype.is::<Float64DataType>() {
        DataType::Float64
    } else if zarr_dtype.is::<RawBitsDataType>() || zarr_dtype.is::<BytesDataType>() {
        DataType::BinaryView
    } else if zarr_dtype.is::<StringDataType>() {
        DataType::Utf8View
    } else if let Some(dt) = zarr_dtype.downcast_ref::<NumpyDateTime64DataType>() {
        match dt.unit {
            NumpyTimeUnit::Millisecond => DataType::Timestamp(TimeUnit::Millisecond, None),
            NumpyTimeUnit::Microsecond => DataType::Timestamp(TimeUnit::Microsecond, None),
            NumpyTimeUnit::Nanosecond => DataType::Timestamp(TimeUnit::Nanosecond, None),
            NumpyTimeUnit::Second => DataType::Timestamp(TimeUnit::Second, None),
            _ => {
                return Err(ZarrDataFusionError::Custom(format!(
                    "Unsupported Numpy datetime64 time unit: {:?}",
                    dt.unit
                )));
            }
        }
    } else {
        return Err(ZarrDataFusionError::Custom(format!(
            "Unsupported Zarr data type: {:?}",
            zarr_dtype
        )));
    };
    Ok(Arc::new(Field::new(&name, data_type, false)))
}

/// Map an Arrow DataType to a Zarr DataType for writing.
/// Returns None for non-scalar types that cannot be stored as 1D Zarr arrays.
pub(crate) fn arrow_to_zarr_dtype(arrow_type: &DataType) -> Option<ZarrDataType> {
    match arrow_type {
        DataType::Boolean => Some(data_type::bool()),
        DataType::Int8 => Some(data_type::int8()),
        DataType::Int16 => Some(data_type::int16()),
        DataType::Int32 => Some(data_type::int32()),
        DataType::Int64 => Some(data_type::int64()),
        DataType::UInt8 => Some(data_type::uint8()),
        DataType::UInt16 => Some(data_type::uint16()),
        DataType::UInt32 => Some(data_type::uint32()),
        DataType::UInt64 => Some(data_type::uint64()),
        DataType::Float32 => Some(data_type::float32()),
        DataType::Float64 => Some(data_type::float64()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            Some(data_type::string())
        }
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
            Some(data_type::bytes())
        }
        DataType::Timestamp(TimeUnit::Second, _) => Some(data_type::numpy_datetime64(
            NumpyTimeUnit::Second,
            std::num::NonZeroU32::new(1).unwrap(),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(data_type::numpy_datetime64(
            NumpyTimeUnit::Millisecond,
            std::num::NonZeroU32::new(1).unwrap(),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => Some(data_type::numpy_datetime64(
            NumpyTimeUnit::Microsecond,
            std::num::NonZeroU32::new(1).unwrap(),
        )),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Some(data_type::numpy_datetime64(
            NumpyTimeUnit::Nanosecond,
            std::num::NonZeroU32::new(1).unwrap(),
        )),
        _ => None,
    }
}

/// Return the default Zarr fill value for a given Zarr DataType.
pub(crate) fn zarr_fill_value(dtype: &ZarrDataType) -> FillValue {
    if dtype.is::<BoolDataType>() {
        FillValue::from(false)
    } else if dtype.is::<Int8DataType>() {
        FillValue::from(0i8)
    } else if dtype.is::<Int16DataType>() {
        FillValue::from(0i16)
    } else if dtype.is::<Int32DataType>() {
        FillValue::from(0i32)
    } else if dtype.is::<Int64DataType>() {
        FillValue::from(0i64)
    } else if dtype.is::<UInt8DataType>() {
        FillValue::from(0u8)
    } else if dtype.is::<UInt16DataType>() {
        FillValue::from(0u16)
    } else if dtype.is::<UInt32DataType>() {
        FillValue::from(0u32)
    } else if dtype.is::<UInt64DataType>() {
        FillValue::from(0u64)
    } else if dtype.is::<Float32DataType>() {
        FillValue::from(0.0f32)
    } else if dtype.is::<Float64DataType>() {
        FillValue::from(0.0f64)
    } else if dtype.is::<StringDataType>() {
        FillValue::from("")
    } else if dtype.is::<BytesDataType>() {
        FillValue::from(vec![])
    } else if dtype.is::<NumpyDateTime64DataType>() {
        FillValue::from(0i64)
    } else {
        FillValue::from(0u8)
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

        assert!(arrow_to_zarr_dtype(&DataType::Boolean).unwrap().is::<BoolDataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Int64).unwrap().is::<Int64DataType>());
        assert!(arrow_to_zarr_dtype(&DataType::UInt32).unwrap().is::<UInt32DataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Float64).unwrap().is::<Float64DataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Utf8).unwrap().is::<StringDataType>());
        assert!(arrow_to_zarr_dtype(&DataType::LargeUtf8).unwrap().is::<StringDataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Utf8View).unwrap().is::<StringDataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Binary).unwrap().is::<BytesDataType>());
        assert!(arrow_to_zarr_dtype(&DataType::BinaryView).unwrap().is::<BytesDataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Timestamp(TimeUnit::Millisecond, None))
            .unwrap()
            .is::<NumpyDateTime64DataType>());
        assert!(arrow_to_zarr_dtype(&DataType::Timestamp(TimeUnit::Second, None))
            .unwrap()
            .is::<NumpyDateTime64DataType>());
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
        let _ = zarr_fill_value(&data_type::int64());
        let _ = zarr_fill_value(&data_type::string());
        let _ = zarr_fill_value(&data_type::bytes());
        let _ = zarr_fill_value(&data_type::numpy_datetime64(
            NumpyTimeUnit::Millisecond,
            std::num::NonZeroU32::new(1).unwrap(),
        ));
    }
}
