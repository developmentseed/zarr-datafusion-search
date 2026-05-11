use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array as ArrowArray, BinaryArray, BinaryViewArray, BooleanArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, ListArray,
    RecordBatch, StringArray, StringViewArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use geo::{Polygon, Rect, coord};
use wkb::writer::{WriteOptions, write_polygon};
use arrow_schema::{DataType as ArrowDataType, Field, Schema, TimeUnit};
use futures::StreamExt;
use stac::api::{ArrowItemsClient, Search, StreamItemsClient};
use zarrs::array::{Array, ArrayBuilder};
use zarrs::array::{ArraySubset, ChunkShapeTraits};
use zarrs::group::Group;
use zarrs::storage::AsyncReadableWritableListableStorageTraits;
use zarrs_storage::StoreKey;

use crate::error::{ZarrDataFusionError, ZarrDataFusionResult};
use crate::schema::{arrow_to_zarr_dtype, zarr_fill_value};

/// Extract the href string for a single asset key from the `assets` StructArray row.
/// Returns "" if the key is absent or the href field is null.
fn extract_href_from_assets(assets_struct: &StructArray, asset_key: &str, row: usize) -> String {
    // Find the column for this asset key
    let asset_col = match assets_struct.column_by_name(asset_key) {
        Some(col) => col,
        None => return String::new(),
    };

    // The field value should be a StructArray with an "href" field
    let asset_struct = match asset_col.as_any().downcast_ref::<StructArray>() {
        Some(s) => s,
        None => return String::new(),
    };

    // Get href column
    let href_col = match asset_struct.column_by_name("href") {
        Some(col) => col,
        None => return String::new(),
    };

    // Extract the string value, falling back to "" for null
    if href_col.is_null(row) {
        return String::new();
    }

    if let Some(arr) = href_col.as_any().downcast_ref::<StringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = href_col.as_any().downcast_ref::<LargeStringArray>() {
        return arr.value(row).to_string();
    }
    if let Some(arr) = href_col.as_any().downcast_ref::<StringViewArray>() {
        return arr.value(row).to_string();
    }

    String::new()
}

/// Build a map of asset_key -> Vec<String> (one href per row) from a RecordBatch.
/// Missing keys or null href fields produce empty strings.
pub(crate) fn extract_asset_hrefs(
    batch: &RecordBatch,
    asset_keys: &[&str],
) -> HashMap<String, Vec<String>> {
    let num_rows = batch.num_rows();
    let mut result: HashMap<String, Vec<String>> = asset_keys
        .iter()
        .map(|k| (k.to_string(), vec![String::new(); num_rows]))
        .collect();

    if asset_keys.is_empty() {
        return result;
    }

    // Find the "assets" column
    let assets_col = match batch.column_by_name("assets") {
        Some(col) => col,
        None => return result, // all keys stay as empty strings
    };

    let assets_struct = match assets_col.as_any().downcast_ref::<StructArray>() {
        Some(s) => s,
        None => return result,
    };

    for key in asset_keys {
        let hrefs = result.get_mut(*key).unwrap();
        for row in 0..num_rows {
            hrefs[row] = extract_href_from_assets(assets_struct, key, row);
        }
    }

    result
}

/// Inspect the existing store at `group_path`.
///
/// Returns `(existing_row_count, effective_chunk_size)`.
/// - If the group is absent or empty: `(0, chunk_size)` (use caller's chunk_size).
/// - If the group has arrays: `(shape[0], array_chunk_size)` (ignore caller's chunk_size).
async fn detect_existing_store(
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
    group_path: &str,
    chunk_size: usize,
) -> ZarrDataFusionResult<(u64, usize)> {
    let group = match Group::async_open(Arc::clone(&store), group_path).await {
        Ok(g) => g,
        Err(_) => return Ok((0, chunk_size)),
    };

    let arrays = group.async_child_arrays().await.unwrap_or_default();

    if arrays.is_empty() {
        return Ok((0, chunk_size));
    }

    // Use the first array to determine existing row count and chunk size
    let first = &arrays[0];
    let row_count = first.shape().first().copied().unwrap_or(0);
    let eff_chunk_size = first
        .chunk_shape(&[0])
        .map(|cs| cs.to_array_shape().first().copied().unwrap_or(chunk_size as u64))
        .unwrap_or(chunk_size as u64) as usize;

    Ok((row_count, eff_chunk_size))
}

/// Patch zarr v3 metadata for bytes arrays so zarr-python can read them.
///
/// zarrs serializes the bytes fill value as a JSON array (e.g. `[]`) but
/// zarr-python's `VariableLengthBytes` expects a base64-encoded string (e.g. `""`).
/// zarrs reads both formats, so this patch is backwards-compatible.
async fn patch_bytes_fill_value(
    store: &dyn AsyncReadableWritableListableStorageTraits,
    array_path: &str,
) -> ZarrDataFusionResult<()> {
    let key_str = format!("{}/zarr.json", array_path.trim_start_matches('/'));
    let key = StoreKey::new(key_str)
        .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;

    let metadata_bytes = store
        .get(&key)
        .await
        .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?
        .ok_or_else(|| ZarrDataFusionError::Custom("Metadata not found".to_string()))?;

    let mut metadata: serde_json::Value = serde_json::from_slice(&metadata_bytes)
        .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;

    if let Some(obj) = metadata.as_object_mut() {
        if let Some(fv) = obj.get("fill_value") {
            if fv.is_array() {
                obj.insert(
                    "fill_value".to_string(),
                    serde_json::Value::String(String::new()),
                );
            }
        }
    }

    let patched = serde_json::to_vec(&metadata)
        .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;

    store
        .set(&key, zarrs_storage::Bytes::from(patched))
        .await
        .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;

    Ok(())
}

/// Write a single Arrow column array to a Zarr 1D array at `array_path`.
///
/// - If the array doesn't exist: creates it with the appropriate shape.
///   For new columns in an existing store (`existing_row_count > 0`), shape is
///   `existing_row_count + num_rows` — rows [0, existing_row_count) return fill value.
/// - If the array already exists: extends the shape and writes at `write_offset`.
async fn write_column_to_zarrs(
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
    array_path: &str,
    column: &dyn ArrowArray,
    arrow_type: &ArrowDataType,
    write_offset: u64,
    existing_row_count: u64,
    effective_chunk_size: usize,
) -> ZarrDataFusionResult<()> {
    let zarr_dtype = arrow_to_zarr_dtype(arrow_type).ok_or_else(|| {
        ZarrDataFusionError::Custom(format!("Unsupported Arrow type for write: {:?}", arrow_type))
    })?;
    let fill_value = zarr_fill_value(&zarr_dtype);
    let num_rows = column.len() as u64;

    // Determine whether array exists
    let array_exists = Array::async_open(Arc::clone(&store), array_path)
        .await
        .is_ok();

    let zarr_array = if array_exists {
        // Extend existing array shape
        let existing = Array::async_open(Arc::clone(&store), array_path).await?;
        let old_shape = existing.shape().to_vec();
        let chunk_sz = existing
            .chunk_shape(&[0])
            .map(|cs| cs.to_array_shape())
            .unwrap_or_else(|_| vec![effective_chunk_size as u64]);
        let arr = ArrayBuilder::new(
            vec![old_shape[0] + num_rows],
            chunk_sz,
            existing.data_type().clone(),
            existing.fill_value().clone(),
        )
        .build(Arc::clone(&store), array_path)?;
        arr.async_store_metadata()
            .await
            .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;
        arr
    } else {
        // New array
        let total_shape = if existing_row_count > 0 {
            existing_row_count + num_rows
        } else {
            num_rows
        };
        let arr = ArrayBuilder::new(
            vec![total_shape],
            vec![effective_chunk_size as u64],
            zarr_dtype,
            fill_value,
        )
        .build(Arc::clone(&store), array_path)?;
        arr.async_store_metadata()
            .await
            .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;
        arr
    };

    // Patch bytes fill value for zarr-python compatibility
    if matches!(
        arrow_type,
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView
    ) {
        patch_bytes_fill_value(store.as_ref(), array_path).await?;
    }

    let subset = ArraySubset::new_with_ranges(&[write_offset..(write_offset + num_rows)]);

    // Dispatch write by Arrow type, converting nulls → fill values
    match arrow_type {
        ArrowDataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            let data: Vec<bool> = (0..arr.len())
                .map(|i| !arr.is_null(i) && arr.value(i))
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Int8 => {
            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
            let data: Vec<i8> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Int16 => {
            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
            let data: Vec<i16> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            let data: Vec<i32> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            let data: Vec<i64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::UInt8 => {
            let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            let data: Vec<u8> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::UInt16 => {
            let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            let data: Vec<u16> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::UInt32 => {
            let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            let data: Vec<u32> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::UInt64 => {
            let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            let data: Vec<u64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
            let data: Vec<f32> = (0..arr.len()).map(|i| if arr.is_null(i) { 0.0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            let data: Vec<f64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0.0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            let data: Vec<String> = (0..arr.len())
                .map(|i| if arr.is_null(i) { String::new() } else { arr.value(i).to_string() })
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            let data: Vec<String> = (0..arr.len())
                .map(|i| if arr.is_null(i) { String::new() } else { arr.value(i).to_string() })
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Utf8View => {
            let arr = column.as_any().downcast_ref::<StringViewArray>().unwrap();
            let data: Vec<String> = (0..arr.len())
                .map(|i| if arr.is_null(i) { String::new() } else { arr.value(i).to_string() })
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Binary => {
            let arr = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            let data: Vec<Vec<u8>> = (0..arr.len())
                .map(|i| if arr.is_null(i) { vec![] } else { arr.value(i).to_vec() })
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::LargeBinary => {
            let arr = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            let data: Vec<Vec<u8>> = (0..arr.len())
                .map(|i| if arr.is_null(i) { vec![] } else { arr.value(i).to_vec() })
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::BinaryView => {
            let arr = column.as_any().downcast_ref::<BinaryViewArray>().unwrap();
            let data: Vec<Vec<u8>> = (0..arr.len())
                .map(|i| if arr.is_null(i) { vec![] } else { arr.value(i).to_vec() })
                .collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Timestamp(TimeUnit::Second, _) => {
            let arr = column.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            let data: Vec<i64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = column.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            let data: Vec<i64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = column.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            let data: Vec<i64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = column.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            let data: Vec<i64> = (0..arr.len()).map(|i| if arr.is_null(i) { 0 } else { arr.value(i) }).collect();
            zarr_array.async_store_array_subset(&subset, &data).await?;
        }
        _ => {
            return Err(ZarrDataFusionError::Custom(format!(
                "write_column_to_zarrs: unsupported type {:?}",
                arrow_type
            )));
        }
    }

    Ok(())
}

/// Ingest STAC search results into a Zarr store's meta group.
pub async fn ingest_stac_search<C>(
    client: &C,
    search: Search,
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
    chunk_size: usize,
    asset_hrefs: &[&str],
) -> ZarrDataFusionResult<u64>
where
    C: ArrowItemsClient,
    C::Error: std::error::Error + Send + Sync + 'static,
{
    const GROUP_PATH: &str = "/meta";

    // Force zarrs to write chunks even when all values equal the fill value.
    zarrs::config::global_config_mut().set_store_empty_chunks(true);

    // 1. Detect existing store state
    let (existing_row_count, effective_chunk_size) =
        detect_existing_store(Arc::clone(&store), GROUP_PATH, chunk_size).await?;

    // 2. Ensure root and meta groups exist
    if Group::async_open(Arc::clone(&store), "/").await.is_err() {
        let root = Group::new_with_metadata(
            Arc::clone(&store),
            "/",
            zarrs::group::GroupMetadata::V3(zarrs::group::GroupMetadataV3::default()),
        )
        .map_err(|e| ZarrDataFusionError::Custom(format!("Failed to create root group: {e}")))?;
        root.async_store_metadata()
            .await
            .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;
    }
    if Group::async_open(Arc::clone(&store), GROUP_PATH).await.is_err() {
        let meta = Group::new_with_metadata(
            Arc::clone(&store),
            GROUP_PATH,
            zarrs::group::GroupMetadata::V3(zarrs::group::GroupMetadataV3::default()),
        )
        .map_err(|e| ZarrDataFusionError::Custom(format!("Failed to create meta group: {e}")))?;
        meta.async_store_metadata()
            .await
            .map_err(|e| ZarrDataFusionError::Custom(e.to_string()))?;
    }

    // 3. Open the RecordBatchReader (synchronous) and stream batches.
    let reader = client
        .search_to_arrow(search)
        .map_err(|e| ZarrDataFusionError::StacSearch(e.to_string()))?;

    // 4. Accumulate batches; flush when buffer reaches effective_chunk_size
    let mut pending_batches: Vec<RecordBatch> = Vec::new();
    let mut pending_rows: usize = 0;
    let mut rows_written: u64 = 0;

    for batch_result in reader {
        let batch = batch_result.map_err(|e| ZarrDataFusionError::Arrow(e.into()))?;
        pending_rows += batch.num_rows();
        pending_batches.push(batch);

        while pending_rows >= effective_chunk_size {
            let write_offset = existing_row_count + rows_written;
            let flushed = flush_pending(
                Arc::clone(&store),
                GROUP_PATH,
                &mut pending_batches,
                &mut pending_rows,
                effective_chunk_size,
                write_offset,
                existing_row_count,
                effective_chunk_size,
                asset_hrefs,
            )
            .await?;
            rows_written += flushed as u64;
        }
    }

    // 5. Flush remainder
    if pending_rows > 0 {
        let remainder_count = pending_rows;
        let write_offset = existing_row_count + rows_written;
        let flushed = flush_pending(
            Arc::clone(&store),
            GROUP_PATH,
            &mut pending_batches,
            &mut pending_rows,
            remainder_count,
            write_offset,
            existing_row_count,
            effective_chunk_size,
            asset_hrefs,
        )
        .await?;
        rows_written += flushed as u64;
    }

    Ok(rows_written)
}

/// An [`ArrowItemsClient`] adapter that wraps a [`stac_io::api::Client`],
/// fetches STAC items over HTTP, and converts them to Arrow via
/// [`stac::geoarrow`].
pub struct HttpArrowClient {
    client: stac_io::api::Client,
    max_items: Option<u64>,
}

impl HttpArrowClient {
    /// Wrap an existing [`stac_io::api::Client`].
    pub fn new(client: stac_io::api::Client, max_items: Option<u64>) -> Self {
        Self { client, max_items }
    }
}

impl ArrowItemsClient for HttpArrowClient {
    type Error = ZarrDataFusionError;
    type RecordBatchStream<'a> = stac::api::RecordBatchReaderAdapter<
        std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>,
    >;

    fn search_to_arrow(
        &self,
        search: Search,
    ) -> Result<Self::RecordBatchStream<'_>, Self::Error> {
        // Bridge from async (search_stream) to sync (ArrowItemsClient).
        let items = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let stream = self
                    .client
                    .search_stream(search)
                    .await
                    .map_err(|e| ZarrDataFusionError::StacSearch(e.to_string()))?;
                futures::pin_mut!(stream);

                let mut items = Vec::new();
                while let Some(result) = stream.next().await {
                    let api_item = result
                        .map_err(|e| ZarrDataFusionError::StacSearch(e.to_string()))?;
                    let item = stac::Item::try_from(api_item)
                        .map_err(|e| ZarrDataFusionError::StacSearch(e.to_string()))?;
                    items.push(item);
                    if let Some(max) = self.max_items {
                        if items.len() as u64 >= max {
                            break;
                        }
                    }
                }
                Ok::<_, ZarrDataFusionError>(items)
            })
        })?;

        if items.is_empty() {
            let schema = Arc::new(arrow_schema::Schema::empty());
            return Ok(stac::api::RecordBatchReaderAdapter::new(
                vec![].into_iter(),
                schema,
            ));
        }

        let (batch, schema) = stac::geoarrow::encode(items)
            .map_err(|e| ZarrDataFusionError::StacSearch(e.to_string()))?;

        Ok(stac::api::RecordBatchReaderAdapter::new(
            vec![Ok(batch)].into_iter(),
            schema,
        ))
    }
}

/// Convenience wrapper: create an [`HttpArrowClient`] and call
/// [`ingest_stac_search`].
pub async fn ingest_stac_api(
    url: &str,
    search: Search,
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
    chunk_size: usize,
    asset_hrefs: &[&str],
    max_items: Option<u64>,
) -> ZarrDataFusionResult<u64> {
    let io_client = stac_io::api::Client::new(url)
        .map_err(|e| ZarrDataFusionError::StacSearch(e.to_string()))?;
    let client = HttpArrowClient::new(io_client, max_items);
    ingest_stac_search(&client, search, store, chunk_size, asset_hrefs).await
}

/// Column names for `proj:transform` after flattening.
///
/// `proj:transform` is a row-major 3x3 affine matrix (6 elements):
///   [a0, a1, a2]     a0 = x pixel scale
///   [a3, a4, a5]     a1 = row rotation
///   [0,  0,  1 ]     a2 = x origin (upper-left)
///                    a3 = column rotation
///                    a4 = y pixel scale (negative)
///                    a5 = y origin (upper-left)
const TRANSFORM_COLUMNS: &[&str] = &[
    "transform_0",
    "transform_1",
    "transform_2",
    "transform_3",
    "transform_4",
    "transform_5",
];

/// Column names for `proj:shape` after flattening (height, width).
const SHAPE_COLUMNS: &[&str] = &["shape_y", "shape_x"];

/// Flatten known list-valued STAC columns into individual scalar columns.
///
/// - `proj:transform` (List<Int64> or List<Float64>, 6 elements) → `transform_a` .. `transform_f`
/// - `proj:shape` (List<Int64>, 2 elements) → `shape_y`, `shape_x`
///
/// Other columns pass through unchanged. Unknown list columns are left as-is
/// (and will be skipped by the downstream writer).
fn flatten_list_columns(batch: &RecordBatch) -> ZarrDataFusionResult<RecordBatch> {
    let schema = batch.schema();
    let mut new_fields: Vec<Arc<Field>> = Vec::new();
    let mut new_columns: Vec<Arc<dyn ArrowArray>> = Vec::new();

    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);

        match field.name().as_str() {
            "bbox" => {
                let bbox_struct = col
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        ZarrDataFusionError::Custom(format!(
                            "Expected bbox to be Struct, got {:?}",
                            field.data_type()
                        ))
                    })?;
                bbox_struct_to_wkb(bbox_struct, &mut new_fields, &mut new_columns)?;
            }
            "proj:transform" => {
                let list = col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        ZarrDataFusionError::Custom(format!(
                            "Expected proj:transform to be List, got {:?}",
                            field.data_type()
                        ))
                    })?;
                // Earth-search uses Int64, but other APIs may use Float64
                flatten_float_list_from_any(list, TRANSFORM_COLUMNS, &mut new_fields, &mut new_columns)?;
            }
            "proj:shape" => {
                let list = col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| {
                        ZarrDataFusionError::Custom(format!(
                            "Expected proj:shape to be List, got {:?}",
                            field.data_type()
                        ))
                    })?;
                flatten_int_list(list, SHAPE_COLUMNS, &mut new_fields, &mut new_columns)?;
            }
            _ => {
                new_fields.push(Arc::clone(field));
                new_columns.push(Arc::clone(col));
            }
        }
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns).map_err(|e| ZarrDataFusionError::Arrow(e))
}

/// Extract each element of a List<Float64> or List<Int64> column into separate Float64 columns.
fn flatten_float_list_from_any(
    list: &ListArray,
    names: &[&str],
    fields: &mut Vec<Arc<Field>>,
    columns: &mut Vec<Arc<dyn ArrowArray>>,
) -> ZarrDataFusionResult<()> {
    let num_rows = list.len();
    for (idx, name) in names.iter().enumerate() {
        let mut values = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            if list.is_null(row) {
                values.push(0.0f64);
            } else {
                let inner = list.value(row);
                let val = if let Some(arr) = inner.as_any().downcast_ref::<Float64Array>() {
                    if idx < arr.len() { arr.value(idx) } else { 0.0 }
                } else if let Some(arr) = inner.as_any().downcast_ref::<Int64Array>() {
                    if idx < arr.len() { arr.value(idx) as f64 } else { 0.0 }
                } else {
                    return Err(ZarrDataFusionError::Custom(format!(
                        "Expected Float64 or Int64 values in {name}, got {:?}",
                        inner.data_type()
                    )));
                };
                values.push(val);
            }
        }
        fields.push(Arc::new(Field::new(*name, ArrowDataType::Float64, false)));
        columns.push(Arc::new(Float64Array::from(values)));
    }
    Ok(())
}

/// Extract each element of a List<Int64> column into separate Int64 columns.
fn flatten_int_list(
    list: &ListArray,
    names: &[&str],
    fields: &mut Vec<Arc<Field>>,
    columns: &mut Vec<Arc<dyn ArrowArray>>,
) -> ZarrDataFusionResult<()> {
    let num_rows = list.len();
    for (idx, name) in names.iter().enumerate() {
        let mut values = Vec::with_capacity(num_rows);
        for row in 0..num_rows {
            if list.is_null(row) {
                values.push(0i64);
            } else {
                let inner = list.value(row);
                let ints = inner.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ZarrDataFusionError::Custom(format!(
                        "Expected Int64 values in {name}, got {:?}",
                        inner.data_type()
                    ))
                })?;
                values.push(if idx < ints.len() {
                    ints.value(idx)
                } else {
                    0
                });
            }
        }
        fields.push(Arc::new(Field::new(*name, ArrowDataType::Int64, false)));
        columns.push(Arc::new(Int64Array::from(values)));
    }
    Ok(())
}

/// Convert a Struct{xmin, ymin, xmax, ymax} bbox column (from `stac::geoarrow::encode`)
/// into a BinaryView column of WKB polygons. Each bbox becomes a closed rectangle
/// polygon via `geo::Rect`.
fn bbox_struct_to_wkb(
    bbox_array: &StructArray,
    fields: &mut Vec<Arc<Field>>,
    columns: &mut Vec<Arc<dyn ArrowArray>>,
) -> ZarrDataFusionResult<()> {
    let get_f64_col = |name: &str| -> ZarrDataFusionResult<&Float64Array> {
        bbox_array
            .column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
            .ok_or_else(|| {
                ZarrDataFusionError::Custom(format!(
                    "bbox struct missing or non-Float64 field '{name}'"
                ))
            })
    };

    let xmin = get_f64_col("xmin")?;
    let ymin = get_f64_col("ymin")?;
    let xmax = get_f64_col("xmax")?;
    let ymax = get_f64_col("ymax")?;

    let num_rows = bbox_array.len();
    let opts = WriteOptions::default();
    let mut wkb_bytes: Vec<Vec<u8>> = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        if bbox_array.is_null(row) {
            wkb_bytes.push(vec![]);
            continue;
        }
        let rect = Rect::new(
            coord! { x: xmin.value(row), y: ymin.value(row) },
            coord! { x: xmax.value(row), y: ymax.value(row) },
        );
        let polygon: Polygon = rect.into();
        let mut buf = Vec::new();
        write_polygon(&mut buf, &polygon, &opts)
            .map_err(|e| ZarrDataFusionError::Custom(format!("WKB encode error: {e}")))?;
        wkb_bytes.push(buf);
    }

    let binary_array = BinaryViewArray::from_iter_values(wkb_bytes.iter().map(|b| b.as_slice()));
    fields.push(Arc::new(Field::new("bbox", ArrowDataType::BinaryView, false)));
    columns.push(Arc::new(binary_array));
    Ok(())
}

/// Concatenate pending batches, flush `flush_count` rows, keep the remainder.
/// Returns the number of rows flushed.
async fn flush_pending(
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
    group_path: &str,
    pending_batches: &mut Vec<RecordBatch>,
    pending_rows: &mut usize,
    flush_count: usize,
    write_offset: u64,
    existing_row_count: u64,
    effective_chunk_size: usize,
    asset_hrefs: &[&str],
) -> ZarrDataFusionResult<usize> {
    // Concatenate all pending batches into one
    let schema = pending_batches[0].schema();
    let combined = arrow::compute::concat_batches(&schema, pending_batches.iter())
        .map_err(|e| ZarrDataFusionError::Arrow(e))?;

    let flush_batch = combined.slice(0, flush_count);
    let remainder = if combined.num_rows() > flush_count {
        Some(combined.slice(flush_count, combined.num_rows() - flush_count))
    } else {
        None
    };

    // Flatten known list columns (proj:transform, proj:shape) into scalars
    let flush_batch = flatten_list_columns(&flush_batch)?;

    // Write each scalar column from the flush batch
    for (i, field) in flush_batch.schema().fields().iter().enumerate() {
        let col = flush_batch.column(i);
        let arrow_type = field.data_type();

        if field.name() == "assets" {
            continue; // Handled separately below
        }

        match arrow_to_zarr_dtype(arrow_type) {
            Some(_) => {
                let array_path = format!("{}/{}", group_path, field.name());
                write_column_to_zarrs(
                    Arc::clone(&store),
                    &array_path,
                    col.as_ref(),
                    arrow_type,
                    write_offset,
                    existing_row_count,
                    effective_chunk_size,
                )
                .await?;
            }
            None => {
                tracing::warn!(
                    column = field.name(),
                    dtype = ?arrow_type,
                    "Skipping non-scalar column"
                );
            }
        }
    }

    // Write asset href columns
    if !asset_hrefs.is_empty() {
        let hrefs = extract_asset_hrefs(&flush_batch, asset_hrefs);
        for (key, values) in &hrefs {
            let col: Arc<dyn ArrowArray> = Arc::new(StringArray::from(
                values.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            ));
            let array_path = format!("{}/asset_{}", group_path, key);
            write_column_to_zarrs(
                Arc::clone(&store),
                &array_path,
                col.as_ref(),
                &ArrowDataType::Utf8,
                write_offset,
                existing_row_count,
                effective_chunk_size,
            )
            .await?;
        }
    }

    // Reset pending state
    *pending_batches = match remainder {
        Some(r) => vec![r],
        None => Vec::new(),
    };
    *pending_rows = pending_batches.iter().map(|b| b.num_rows()).sum();

    Ok(flush_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use std::sync::Arc;

    use object_store::local::LocalFileSystem;
    use stac::api::{ArrowItemsClient, Search};
    use stac::api::RecordBatchReaderAdapter;
    use tempfile::TempDir;
    use zarrs::array::{Array, ArrayBuilder, data_type, FillValue};
    use zarrs::array::{ArraySubset, ChunkShapeTraits};
    use zarrs::group::{GroupMetadata, GroupMetadataV3};

    struct MockClient {
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
    }

    impl ArrowItemsClient for MockClient {
        type Error = arrow_schema::ArrowError;
        type RecordBatchStream<'a>
            = RecordBatchReaderAdapter<
            std::vec::IntoIter<Result<RecordBatch, arrow_schema::ArrowError>>,
        >
        where
            Self: 'a;

        fn search_to_arrow(
            &self,
            _search: Search,
        ) -> Result<Self::RecordBatchStream<'_>, Self::Error> {
            let results: Vec<Result<RecordBatch, arrow_schema::ArrowError>> =
                self.batches.clone().into_iter().map(Ok).collect();
            Ok(RecordBatchReaderAdapter::new(results.into_iter(), self.schema.clone()))
        }
    }

    fn make_stac_batch(ids: &[&str], datetimes: &[i64], collections: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "datetime",
                DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("collection", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids.to_vec())),
                Arc::new(TimestampMillisecondArray::from(datetimes.to_vec())),
                Arc::new(StringArray::from(collections.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_ingest_stac_search_creates_meta_arrays() {
        let (store, _dir) = make_test_store().await;

        let batch1 = make_stac_batch(
            &["item-0", "item-1", "item-2"],
            &[1_000_000, 2_000_000, 3_000_000],
            &["sentinel-2", "sentinel-2", "sentinel-2"],
        );
        let schema = batch1.schema();
        let client = MockClient {
            batches: vec![batch1],
            schema,
        };

        let rows_written = ingest_stac_search(
            &client,
            Search::default(),
            Arc::clone(&store),
            100,
            &[],
        )
        .await
        .unwrap();

        assert_eq!(rows_written, 3);

        // Verify id array was written
        let id_arr = Array::async_open(Arc::clone(&store), "/meta/id")
            .await
            .unwrap();
        assert_eq!(id_arr.shape(), &[3u64]);
        let ids: Vec<String> = id_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![3u64]))
            .await
            .unwrap();
        assert_eq!(ids, vec!["item-0", "item-1", "item-2"]);

        // Verify collection array was written
        let coll_arr = Array::async_open(Arc::clone(&store), "/meta/collection")
            .await
            .unwrap();
        let colls: Vec<String> = coll_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![3u64]))
            .await
            .unwrap();
        assert_eq!(colls, vec!["sentinel-2", "sentinel-2", "sentinel-2"]);
    }

    #[tokio::test]
    async fn test_ingest_stac_search_chunk_accumulation() {
        // chunk_size=2, 5 rows across 2 batches → should produce correct data
        let (store, _dir) = make_test_store().await;

        let batch1 = make_stac_batch(
            &["a", "b", "c"],
            &[1, 2, 3],
            &["col", "col", "col"],
        );
        let batch2 = make_stac_batch(
            &["d", "e"],
            &[4, 5],
            &["col", "col"],
        );
        let schema = batch1.schema();
        let client = MockClient {
            batches: vec![batch1, batch2],
            schema,
        };

        let rows_written = ingest_stac_search(
            &client,
            Search::default(),
            Arc::clone(&store),
            2, // chunk_size=2
            &[],
        )
        .await
        .unwrap();

        assert_eq!(rows_written, 5);

        let id_arr = Array::async_open(Arc::clone(&store), "/meta/id")
            .await
            .unwrap();
        assert_eq!(id_arr.shape(), &[5u64]);
        let ids: Vec<String> = id_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![5u64]))
            .await
            .unwrap();
        assert_eq!(ids, vec!["a", "b", "c", "d", "e"]);
    }

    #[tokio::test]
    async fn test_ingest_stac_search_appends_to_existing_store() {
        let (store, _dir) = make_test_store().await;

        // First ingest: 3 rows
        let batch1 = make_stac_batch(
            &["a", "b", "c"],
            &[1, 2, 3],
            &["col", "col", "col"],
        );
        let schema = batch1.schema();
        let client1 = MockClient { batches: vec![batch1], schema: schema.clone() };
        ingest_stac_search(&client1, Search::default(), Arc::clone(&store), 100, &[])
            .await
            .unwrap();

        // Second ingest: 2 more rows — chunk_size param is ignored, uses existing chunk_size=100
        let batch2 = make_stac_batch(
            &["d", "e"],
            &[4, 5],
            &["col", "col"],
        );
        let client2 = MockClient { batches: vec![batch2], schema };
        let rows = ingest_stac_search(&client2, Search::default(), Arc::clone(&store), 999, &[])
            .await
            .unwrap();

        assert_eq!(rows, 2);

        let id_arr = Array::async_open(Arc::clone(&store), "/meta/id").await.unwrap();
        assert_eq!(id_arr.shape(), &[5u64]);
        let ids: Vec<String> = id_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![5u64]))
            .await
            .unwrap();
        assert_eq!(ids, vec!["a", "b", "c", "d", "e"]);
    }

    #[tokio::test]
    async fn test_ingest_stac_search_extracts_asset_hrefs() {
        let (store, _dir) = make_test_store().await;

        let batch = make_batch_with_assets();
        let schema = batch.schema();
        let client = MockClient { batches: vec![batch], schema };

        let rows = ingest_stac_search(
            &client,
            Search::default(),
            Arc::clone(&store),
            100,
            &["B01", "thumbnail"],
        )
        .await
        .unwrap();

        assert_eq!(rows, 2);

        let b01_arr = Array::async_open(Arc::clone(&store), "/meta/asset_B01")
            .await
            .unwrap();
        let hrefs: Vec<String> = b01_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![2u64]))
            .await
            .unwrap();
        assert_eq!(hrefs, vec!["s3://bucket/b01_0.tif", "s3://bucket/b01_1.tif"]);

        let thumb_arr = Array::async_open(Arc::clone(&store), "/meta/asset_thumbnail")
            .await
            .unwrap();
        let thumbs: Vec<String> = thumb_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![2u64]))
            .await
            .unwrap();
        assert_eq!(thumbs, vec!["s3://bucket/thumb_0.jpg", ""]);
    }

    async fn make_test_store() -> (Arc<dyn AsyncReadableWritableListableStorageTraits>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let local_fs = LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap();
        let store: Arc<dyn AsyncReadableWritableListableStorageTraits> =
            Arc::new(zarrs_object_store::AsyncObjectStore::new(local_fs));
        (store, temp_dir)
    }

    #[tokio::test]
    async fn test_detect_empty_store_returns_zero_row_count() {
        let (store, _dir) = make_test_store().await;
        let (row_count, eff_chunk_size) =
            detect_existing_store(store, "/meta", 500).await.unwrap();
        assert_eq!(row_count, 0);
        assert_eq!(eff_chunk_size, 500); // uses provided chunk_size when store is empty
    }

    #[tokio::test]
    async fn test_detect_existing_store_returns_correct_row_count_and_chunk_size() {
        let (store, _dir) = make_test_store().await;

        // Write a root group, meta group, and one array of length 300 with chunk_size 100
        let root = Group::new_with_metadata(
            Arc::clone(&store),
            "/",
            GroupMetadata::V3(GroupMetadataV3::default()),
        )
        .unwrap();
        root.async_store_metadata().await.unwrap();
        let meta = Group::new_with_metadata(
            Arc::clone(&store),
            "/meta",
            GroupMetadata::V3(GroupMetadataV3::default()),
        )
        .unwrap();
        meta.async_store_metadata().await.unwrap();

        let arr = ArrayBuilder::new(
            vec![300u64],
            vec![100u64],
            data_type::int64(),
            FillValue::from(0i64),
        )
        .build(Arc::clone(&store), "/meta/count")
        .unwrap();
        arr.async_store_metadata().await.unwrap();
        arr.async_store_array_subset(
            &ArraySubset::new_with_shape(vec![300u64]),
            &vec![0i64; 300],
        )
        .await
        .unwrap();

        let (row_count, eff_chunk_size) =
            detect_existing_store(Arc::clone(&store), "/meta", 999).await.unwrap();
        assert_eq!(row_count, 300);
        assert_eq!(eff_chunk_size, 100); // uses existing chunk_size, not provided 999
    }

    fn make_batch_with_assets() -> RecordBatch {
        // assets struct: { B01: { href: ... }, thumbnail: { href: ... } }
        // Row 0: B01.href = "s3://bucket/b01_0.tif", thumbnail.href = "s3://bucket/thumb_0.jpg"
        // Row 1: B01.href = "s3://bucket/b01_1.tif", thumbnail.href = null (missing)
        use arrow_schema::Fields;

        let b01_href = Arc::new(StringArray::from(vec![
            Some("s3://bucket/b01_0.tif"),
            Some("s3://bucket/b01_1.tif"),
        ]));
        let b01_struct = StructArray::from(vec![(
            Arc::new(Field::new("href", DataType::Utf8, true)),
            Arc::clone(&b01_href) as Arc<dyn ArrowArray>,
        )]);

        let thumb_href = Arc::new(StringArray::from(vec![
            Some("s3://bucket/thumb_0.jpg"),
            None::<&str>,
        ]));
        let thumb_struct = StructArray::from(vec![(
            Arc::new(Field::new("href", DataType::Utf8, true)),
            Arc::clone(&thumb_href) as Arc<dyn ArrowArray>,
        )]);

        let assets_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("B01", DataType::Struct(Fields::from(vec![
                    Field::new("href", DataType::Utf8, true),
                ])), true)),
                Arc::new(b01_struct) as Arc<dyn ArrowArray>,
            ),
            (
                Arc::new(Field::new("thumbnail", DataType::Struct(Fields::from(vec![
                    Field::new("href", DataType::Utf8, true),
                ])), true)),
                Arc::new(thumb_struct) as Arc<dyn ArrowArray>,
            ),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "assets",
                DataType::Struct(assets_struct.fields().clone()),
                true,
            ),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["item-0", "item-1"])),
                Arc::new(assets_struct),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_extract_asset_hrefs_present_key() {
        let batch = make_batch_with_assets();
        let result = extract_asset_hrefs(&batch, &["B01"]);
        assert_eq!(result["B01"], vec!["s3://bucket/b01_0.tif", "s3://bucket/b01_1.tif"]);
    }

    #[test]
    fn test_extract_asset_hrefs_null_href_becomes_empty_string() {
        let batch = make_batch_with_assets();
        let result = extract_asset_hrefs(&batch, &["thumbnail"]);
        assert_eq!(result["thumbnail"], vec!["s3://bucket/thumb_0.jpg", ""]);
    }

    #[test]
    fn test_extract_asset_hrefs_missing_key_returns_empty_strings() {
        let batch = make_batch_with_assets();
        let result = extract_asset_hrefs(&batch, &["nonexistent"]);
        assert_eq!(result["nonexistent"], vec!["", ""]);
    }

    #[tokio::test]
    async fn test_write_column_creates_new_int64_array() {
        let (store, _dir) = make_test_store().await;

        // Create groups
        let root = Group::new_with_metadata(
            Arc::clone(&store), "/",
            GroupMetadata::V3(GroupMetadataV3::default()),
        ).unwrap();
        root.async_store_metadata().await.unwrap();
        let meta = Group::new_with_metadata(
            Arc::clone(&store), "/meta",
            GroupMetadata::V3(GroupMetadataV3::default()),
        ).unwrap();
        meta.async_store_metadata().await.unwrap();

        let data = vec![10i64, 20, 30, 40, 50];
        let col: Arc<dyn ArrowArray> = Arc::new(Int64Array::from(data.clone()));

        write_column_to_zarrs(
            Arc::clone(&store),
            "/meta/count",
            &col,
            &ArrowDataType::Int64,
            0,   // write_offset
            0,   // existing_row_count (new store)
            200, // effective_chunk_size
        )
        .await
        .unwrap();

        // Read back and verify
        let arr = Array::async_open(Arc::clone(&store), "/meta/count")
            .await
            .unwrap();
        assert_eq!(arr.shape(), &[5u64]);
        let read_back: Vec<i64> = arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![5u64]))
            .await
            .unwrap();
        assert_eq!(read_back, data);
    }

    #[tokio::test]
    async fn test_write_column_extends_existing_array() {
        let (store, _dir) = make_test_store().await;

        let root = Group::new_with_metadata(
            Arc::clone(&store), "/",
            GroupMetadata::V3(GroupMetadataV3::default()),
        ).unwrap();
        root.async_store_metadata().await.unwrap();
        let meta = Group::new_with_metadata(
            Arc::clone(&store), "/meta",
            GroupMetadata::V3(GroupMetadataV3::default()),
        ).unwrap();
        meta.async_store_metadata().await.unwrap();

        // First write: 3 rows
        let initial_col: Arc<dyn ArrowArray> = Arc::new(Int64Array::from(vec![1i64, 2, 3]));
        write_column_to_zarrs(
            Arc::clone(&store),
            "/meta/val",
            &initial_col,
            &ArrowDataType::Int64,
            0, 0, 200,
        )
        .await
        .unwrap();

        // Append 2 more rows
        let append_col: Arc<dyn ArrowArray> = Arc::new(Int64Array::from(vec![4i64, 5]));
        write_column_to_zarrs(
            Arc::clone(&store),
            "/meta/val",
            &append_col,
            &ArrowDataType::Int64,
            3,   // write_offset = existing row count
            3,   // existing_row_count
            200,
        )
        .await
        .unwrap();

        let arr = Array::async_open(Arc::clone(&store), "/meta/val")
            .await
            .unwrap();
        assert_eq!(arr.shape(), &[5u64]);
        let read_back: Vec<i64> = arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![5u64]))
            .await
            .unwrap();
        assert_eq!(read_back, vec![1i64, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_ingest_proj_transform_writes_zero_valued_columns() {
        // Verifies that transform columns with all-zero values (rotation coefficients)
        // are actually written and readable, not skipped due to fill-value optimization.
        use arrow_array::{builder::ListBuilder, Float64Array as F64Array};

        let (store, _dir) = make_test_store().await;

        // Build a batch with proj:transform as List<Float64>
        // Typical non-rotated transform: [10.0, 0.0, 399960.0, 0.0, -10.0, 4500000.0]
        let mut list_builder = ListBuilder::new(arrow_array::builder::Float64Builder::new());
        // Row 0
        list_builder.values().append_value(10.0);
        list_builder.values().append_value(0.0);
        list_builder.values().append_value(399960.0);
        list_builder.values().append_value(0.0);
        list_builder.values().append_value(-10.0);
        list_builder.values().append_value(4500000.0);
        list_builder.append(true);
        // Row 1
        list_builder.values().append_value(20.0);
        list_builder.values().append_value(0.0);
        list_builder.values().append_value(500000.0);
        list_builder.values().append_value(0.0);
        list_builder.values().append_value(-20.0);
        list_builder.values().append_value(6000000.0);
        list_builder.append(true);

        let transform_col = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "proj:transform",
                transform_col.data_type().clone(),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["item-0", "item-1"])),
                Arc::new(transform_col),
            ],
        )
        .unwrap();

        let client = MockClient {
            batches: vec![batch],
            schema,
        };

        let rows = ingest_stac_search(
            &client,
            Search::default(),
            Arc::clone(&store),
            100,
            &[],
        )
        .await
        .unwrap();
        assert_eq!(rows, 2);

        // Verify ALL transform columns are written and readable, including the all-zero ones.
        for (col_name, expected) in [
            ("transform_0", vec![10.0, 20.0]),
            ("transform_1", vec![0.0, 0.0]),   // rotation — all zeros
            ("transform_2", vec![399960.0, 500000.0]),
            ("transform_3", vec![0.0, 0.0]),   // rotation — all zeros
            ("transform_4", vec![-10.0, -20.0]),
            ("transform_5", vec![4500000.0, 6000000.0]),
        ] {
            let arr = Array::async_open(Arc::clone(&store), &format!("/meta/{col_name}"))
                .await
                .unwrap_or_else(|_| panic!("Array /meta/{col_name} should exist"));
            let data: Vec<f64> = arr
                .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![2u64]))
                .await
                .unwrap_or_else(|_| panic!("Failed to read {col_name}"));
            assert_eq!(data, expected, "Mismatch for {col_name}");
        }
    }

    #[tokio::test]
    async fn test_write_column_new_column_in_existing_store_has_fill_values() {
        let (store, _dir) = make_test_store().await;

        let root = Group::new_with_metadata(
            Arc::clone(&store), "/",
            GroupMetadata::V3(GroupMetadataV3::default()),
        ).unwrap();
        root.async_store_metadata().await.unwrap();
        let meta = Group::new_with_metadata(
            Arc::clone(&store), "/meta",
            GroupMetadata::V3(GroupMetadataV3::default()),
        ).unwrap();
        meta.async_store_metadata().await.unwrap();

        // New column in a store with existing_row_count=3, writing 2 new rows
        let col: Arc<dyn ArrowArray> = Arc::new(Int64Array::from(vec![100i64, 200]));
        write_column_to_zarrs(
            Arc::clone(&store),
            "/meta/new_col",
            &col,
            &ArrowDataType::Int64,
            3,   // write_offset starts after the existing rows
            3,   // existing_row_count = 3, so rows [0,3) will be fill values
            200,
        )
        .await
        .unwrap();

        let arr = Array::async_open(Arc::clone(&store), "/meta/new_col")
            .await
            .unwrap();
        assert_eq!(arr.shape(), &[5u64]); // 3 fill + 2 real
        let read_back: Vec<i64> = arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![5u64]))
            .await
            .unwrap();
        // First 3 are fill value (0), last 2 are real data
        assert_eq!(read_back, vec![0i64, 0, 0, 100, 200]);
    }

    #[tokio::test]
    async fn test_ingest_bbox_writes_wkb_polygons() {
        use arrow_schema::Fields;
        use geo::{Polygon, Rect, coord};
        use wkb::writer::{WriteOptions, write_polygon};

        let (store, _dir) = make_test_store().await;

        // Build a bbox struct column matching stac::geoarrow::encode output:
        // Struct { xmin: Float64, ymin: Float64, xmax: Float64, ymax: Float64 }
        let bbox_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("xmin", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![-10.0, -125.0])) as Arc<dyn ArrowArray>,
            ),
            (
                Arc::new(Field::new("ymin", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![35.0, 25.0])) as Arc<dyn ArrowArray>,
            ),
            (
                Arc::new(Field::new("xmax", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![30.0, -65.0])) as Arc<dyn ArrowArray>,
            ),
            (
                Arc::new(Field::new("ymax", DataType::Float64, true)),
                Arc::new(Float64Array::from(vec![60.0, 50.0])) as Arc<dyn ArrowArray>,
            ),
        ]);

        let bbox_fields = Fields::from(vec![
            Field::new("xmin", DataType::Float64, true),
            Field::new("ymin", DataType::Float64, true),
            Field::new("xmax", DataType::Float64, true),
            Field::new("ymax", DataType::Float64, true),
        ]);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("bbox", DataType::Struct(bbox_fields), true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["item-0", "item-1"])),
                Arc::new(bbox_struct),
            ],
        )
        .unwrap();

        let client = MockClient {
            batches: vec![batch],
            schema,
        };

        let rows = ingest_stac_search(
            &client,
            Search::default(),
            Arc::clone(&store),
            100,
            &[],
        )
        .await
        .unwrap();
        assert_eq!(rows, 2);

        // Verify bbox array exists and contains valid WKB
        let bbox_arr = Array::async_open(Arc::clone(&store), "/meta/bbox")
            .await
            .expect("bbox array should exist");
        assert_eq!(bbox_arr.shape(), &[2u64]);

        let wkb_data: Vec<Vec<u8>> = bbox_arr
            .async_retrieve_array_subset_elements(&ArraySubset::new_with_shape(vec![2u64]))
            .await
            .unwrap();
        assert_eq!(wkb_data.len(), 2);
        assert!(!wkb_data[0].is_empty(), "WKB bytes should not be empty");
        assert!(!wkb_data[1].is_empty(), "WKB bytes should not be empty");

        // Verify the WKB matches what we'd generate directly from the same bbox
        let expected_rect = Rect::new(
            coord! { x: -10.0, y: 35.0 },
            coord! { x: 30.0, y: 60.0 },
        );
        let expected_polygon: Polygon = expected_rect.into();
        let mut expected_buf = Vec::new();
        write_polygon(&mut expected_buf, &expected_polygon, &WriteOptions::default()).unwrap();
        assert_eq!(wkb_data[0], expected_buf, "WKB for row 0 should match expected polygon");
    }
}
