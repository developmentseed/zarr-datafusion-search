//! Generate an Arrow schema from a Zarr array schema.

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use datafusion::common::HashMap;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array::data_type::DataType as ZarrDataType;
use zarrs::metadata_ext::data_type::NumpyTimeUnit;
use zarrs::storage::{AsyncReadableListableStorageTraits, ReadableListableStorageTraits};

use crate::error::{ZarrDataFusionError, ZarrDataFusionResult};

/// Inspects 1D Zarr arrays in a group and generates an Arrow schema.
///
/// This function will:
/// - List all arrays in the specified group path
/// - Filter to only include 1D arrays
/// - Map Zarr data types to Arrow data types
/// - Generate an Arrow schema with fields for each array
///
/// # Arguments
/// * `storage` - A Zarr storage backend (filesystem or object store)
/// * `group_path` - Path to the Zarr group (e.g., "/meta")
///
/// # Returns
/// An Arrow `Schema` containing fields for each 1D array in the group
pub fn schema_from_zarr_group<S>(storage: Arc<S>, group_path: &str) -> ZarrDataFusionResult<Schema>
where
    S: ReadableListableStorageTraits + ?Sized + 'static,
{
    // Normalize group path: remove leading and trailing slashes
    let group_path = group_path.trim_start_matches('/').trim_end_matches('/');

    // List all keys in the group
    let keys = storage.list()?;

    // Find array paths in the group
    let mut fields = Vec::new();

    for key in keys.iter() {
        let key_str = key.as_str();

        // Check if this key is an array metadata file in our group
        if key_str.starts_with(group_path) && key_str.ends_with("/zarr.json") {
            // Extract array name from path
            // e.g., "meta/collection/zarr.json" -> "collection"
            let relative_path = match key_str
                .strip_prefix(group_path)
                .and_then(|s| s.trim_start_matches('/').strip_suffix("/zarr.json"))
            {
                Some(path) if !path.is_empty() => path,
                _ => continue, // Skip if this is the group itself or invalid
            };

            // Skip if this is a nested group (contains '/')
            if relative_path.contains('/') {
                continue;
            }

            // Construct the full array path with leading slash for zarrs
            let array_path = format!("/{}/{}", group_path, relative_path);

            // Try to open the array to get its metadata
            match Array::open(storage.clone(), &array_path) {
                Ok(array) => {
                    // Only include 1D arrays
                    if array.shape().len() == 1 {
                        let field_name = relative_path.to_string();

                        match zarr_to_arrow_dtype(array.data_type()) {
                            Ok(arrow_dtype) => {
                                // For now, mark all fields as nullable (false means non-nullable)
                                // In the future, we could check Zarr metadata for nullability
                                fields.push(Field::new(field_name, arrow_dtype, false));
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: Could not convert Zarr dtype for array '{}': {}",
                                    field_name, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Could not open array '{}': {}", relative_path, e);
                }
            }
        }
    }

    if fields.is_empty() {
        return Err(ZarrDataFusionError::Custom(format!(
            "No 1D arrays found in group '{}'",
            group_path
        )));
    }

    Ok(Schema::new(fields))
}

/// Async version of schema inspection for async storage backends
pub async fn schema_from_zarr_group_async<S>(
    storage: Arc<S>,
    group_path: &str,
) -> ZarrDataFusionResult<Schema>
where
    S: AsyncReadableListableStorageTraits + ?Sized + 'static,
{
    // Normalize group path: remove leading and trailing slashes
    let group_path = group_path.trim_start_matches('/').trim_end_matches('/');

    // List all keys in the group
    let keys = storage.list().await?;

    // Find array paths in the group
    let mut fields = Vec::new();

    for key in keys.iter() {
        let key_str = key.as_str();

        // Check if this key is an array metadata file in our group
        if key_str.starts_with(group_path) && key_str.ends_with("/zarr.json") {
            // Extract array name from path
            let relative_path = match key_str
                .strip_prefix(group_path)
                .and_then(|s| s.trim_start_matches('/').strip_suffix("/zarr.json"))
            {
                Some(path) if !path.is_empty() => path,
                _ => continue, // Skip if this is the group itself or invalid
            };

            // Skip if this is a nested group
            if relative_path.contains('/') {
                continue;
            }

            // Construct the full array path with leading slash for zarrs
            let array_path = format!("/{}/{}", group_path, relative_path);

            // Try to open the array to get its metadata
            match Array::async_open(storage.clone(), &array_path).await {
                Ok(array) => {
                    // Only include 1D arrays
                    if array.shape().len() == 1 {
                        let field_name = relative_path.to_string();

                        match zarr_to_arrow_dtype(array.data_type()) {
                            Ok(arrow_dtype) => {
                                fields.push(Field::new(field_name, arrow_dtype, false));
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: Could not convert Zarr dtype for array '{}': {}",
                                    field_name, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Could not open array '{}': {}", relative_path, e);
                }
            }
        }
    }

    if fields.is_empty() {
        return Err(ZarrDataFusionError::Custom(format!(
            "No 1D arrays found in group '{}'",
            group_path
        )));
    }

    Ok(Schema::new(fields))
}

#[derive(Clone, Debug)]
struct ArrayMetadata {
    shape: Vec<u64>,
    dtype: ZarrDataType,
}

#[derive(Clone, Debug)]
struct GroupArrays(HashMap<String, ArrayMetadata>);

fn load_group_arrays<S: ReadableListableStorageTraits>(
    storage: Arc<S>,
    group_path: &str,
) -> ZarrDataFusionResult<GroupArrays> {
    // Normalize group path: remove leading and trailing slashes
    let group_path = group_path.trim_start_matches('/').trim_end_matches('/');

    for store_key in storage.list() {
        Array::open(storage.clone(), path)
    }

    Ok(GroupArrays(HashMap::new()))
}

pub fn schema_from_zarr_group2<S>(storage: Arc<S>, group_path: &str) -> ZarrDataFusionResult<Schema>
where
    S: ReadableListableStorageTraits + ?Sized + 'static,
{
    // Normalize group path: remove leading and trailing slashes
    let group_path = group_path.trim_start_matches('/').trim_end_matches('/');

    // List all keys in the group
    let keys = storage.list()?;

    // Find array paths in the group
    let mut fields = Vec::new();

    for key in keys.iter() {
        let key_str = key.as_str();

        // Check if this key is an array metadata file in our group
        if key_str.starts_with(group_path) && key_str.ends_with("/zarr.json") {
            // Extract array name from path
            // e.g., "meta/collection/zarr.json" -> "collection"
            let relative_path = match key_str
                .strip_prefix(group_path)
                .and_then(|s| s.trim_start_matches('/').strip_suffix("/zarr.json"))
            {
                Some(path) if !path.is_empty() => path,
                _ => continue, // Skip if this is the group itself or invalid
            };

            // Skip if this is a nested group (contains '/')
            if relative_path.contains('/') {
                continue;
            }

            // Construct the full array path with leading slash for zarrs
            let array_path = format!("/{}/{}", group_path, relative_path);

            // Try to open the array to get its metadata
            match Array::open(storage.clone(), &array_path) {
                Ok(array) => {
                    // Only include 1D arrays
                    if array.shape().len() == 1 {
                        let field_name = relative_path.to_string();

                        match zarr_to_arrow_dtype(array.data_type()) {
                            Ok(arrow_dtype) => {
                                // For now, mark all fields as nullable (false means non-nullable)
                                // In the future, we could check Zarr metadata for nullability
                                fields.push(Field::new(field_name, arrow_dtype, false));
                            }
                            Err(e) => {
                                eprintln!(
                                    "Warning: Could not convert Zarr dtype for array '{}': {}",
                                    field_name, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Warning: Could not open array '{}': {}", relative_path, e);
                }
            }
        }
    }

    if fields.is_empty() {
        return Err(ZarrDataFusionError::Custom(format!(
            "No 1D arrays found in group '{}'",
            group_path
        )));
    }

    Ok(Schema::new(fields))
}

fn validate_group() {}

/// Maps a Zarr data type to an Arrow data type
fn zarr_to_arrow_dtype(zarr_dtype: &ZarrDataType) -> ZarrDataFusionResult<DataType> {
    match zarr_dtype {
        ZarrDataType::Bool => Ok(DataType::Boolean),
        ZarrDataType::Int8 => Ok(DataType::Int8),
        ZarrDataType::Int16 => Ok(DataType::Int16),
        ZarrDataType::Int32 => Ok(DataType::Int32),
        ZarrDataType::Int64 => Ok(DataType::Int64),
        ZarrDataType::UInt8 => Ok(DataType::UInt8),
        ZarrDataType::UInt16 => Ok(DataType::UInt16),
        ZarrDataType::UInt32 => Ok(DataType::UInt32),
        ZarrDataType::UInt64 => Ok(DataType::UInt64),
        ZarrDataType::Float16 => Ok(DataType::Float16),
        ZarrDataType::Float32 => Ok(DataType::Float32),
        ZarrDataType::Float64 => Ok(DataType::Float64),
        ZarrDataType::Complex64 | ZarrDataType::Complex128 => Err(ZarrDataFusionError::Custom(
            "Complex64/Complex128 not yet supported.".to_string(),
        )),
        ZarrDataType::RawBits(_size) => Ok(DataType::BinaryView),
        ZarrDataType::String => Ok(DataType::Utf8View),
        ZarrDataType::NumpyDateTime64 {
            unit,
            scale_factor: _,
        } => match unit {
            NumpyTimeUnit::Millisecond => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
            NumpyTimeUnit::Microsecond => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
            NumpyTimeUnit::Nanosecond => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
            NumpyTimeUnit::Second => Ok(DataType::Timestamp(TimeUnit::Second, None)),
            _ => Err(ZarrDataFusionError::Custom(format!(
                "Unsupported Numpy datetime64 time unit: {:?}",
                unit
            ))),
        },
        ZarrDataType::Extension(ext) => {
            // Check if this is a datetime extension
            if ext.name() == "datetime" {
                // Zarr datetime64[ms] -> Arrow Timestamp(Millisecond)
                // This matches the pattern used in table_provider.rs
                Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
            } else {
                Err(ZarrDataFusionError::Custom(format!(
                    "Unsupported Zarr extension type: {}",
                    ext.name()
                )))
            }
        }
        _ => Err(ZarrDataFusionError::Custom(format!(
            "Unsupported Zarr data type: {:?}",
            zarr_dtype
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use zarrs_filesystem::FilesystemStore;

    #[test]
    fn test_schema_from_zarr_group() {
        let store = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

        let schema = schema_from_zarr_group(store, "/meta").unwrap();

        // Should have 3 fields: collection, date, bbox
        assert_eq!(schema.fields().len(), 3);

        // Check field names (may be in any order)
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(field_names.contains(&"collection"));
        assert!(field_names.contains(&"date"));
        assert!(field_names.contains(&"bbox"));

        // Check specific field types
        let collection_field = schema.field_with_name("collection").unwrap();
        assert_eq!(collection_field.data_type(), &DataType::Utf8);

        let date_field = schema.field_with_name("date").unwrap();
        assert_eq!(
            date_field.data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, None)
        );

        let bbox_field = schema.field_with_name("bbox").unwrap();
        assert_eq!(bbox_field.data_type(), &DataType::Utf8);
    }
}
