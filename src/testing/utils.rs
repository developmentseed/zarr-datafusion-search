use icechunk::{ObjectStorage, Repository};
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use zarrs::array::{ArrayBuilder, DataType, FillValue};
use zarrs::array_subset::ArraySubset;
use zarrs::metadata_ext::data_type::NumpyTimeUnit;
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::AsyncReadableWritableListableStorageTraits;

pub(crate) struct LocalZarrStoreWrapper {
    _temp_dir: TempDir,
    store: Arc<AsyncObjectStore<LocalFileSystem>>,
    path: PathBuf,
}

#[cfg(test)]
impl LocalZarrStoreWrapper {
    pub(crate) fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();
        let store = AsyncObjectStore::new(LocalFileSystem::new_with_prefix(&path).unwrap());
        Self {
            _temp_dir: temp_dir,
            store: Arc::new(store),
            path,
        }
    }

    pub(crate) fn get_store(&self) -> Arc<AsyncObjectStore<LocalFileSystem>> {
        self.store.clone()
    }

    pub(crate) fn get_store_path(&self) -> String {
        self.path.as_os_str().to_str().unwrap().into()
    }
}

pub(crate) struct LocalIcechunkStoreWrapper {
    _temp_dir: TempDir,
    store: Arc<AsyncIcechunkStore>,
    path: PathBuf,
}

#[cfg(test)]
impl LocalIcechunkStoreWrapper {
    pub(crate) async fn new() -> Self {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();
        let repo = Repository::create(
            None,
            Arc::new(ObjectStorage::new_local_filesystem(&path).await.unwrap()),
            HashMap::new(),
        )
        .await
        .unwrap();
        let session = repo.writable_session("main").await.unwrap();
        Self {
            _temp_dir: temp_dir,
            store: Arc::new(AsyncIcechunkStore::new(session)),
            path,
        }
    }

    pub(crate) fn get_store(&self) -> Arc<AsyncIcechunkStore> {
        self.store.clone()
    }

    pub(crate) fn get_store_path(&self) -> String {
        self.path.to_str().unwrap().into()
    }
}

/// Creates three arrays in /meta group:
/// - date: datetime64[ms] array with dates [2023-01-01, 2023-01-02, 2023-01-03]
/// - collection: variable length UTF8 array with ["collection_a", "collection_b", "collection_c"]
/// - bbox: variable length bytes array with WKB-encoded boxes
///
/// Optionally creates R-tree spatial index in /indexes/bbox if `include_geoindex` is true
#[cfg(test)]
pub(crate) async fn generate_test_data_arrays(
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
    include_geoindex: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    use chrono::NaiveDate;

    // Zarrs requires explicit group creation to
    // avoid implicit group issues.
    let group = zarrs::group::GroupBuilder::new().build(store.clone(), "/")?;
    group.async_store_metadata().await?;

    let group = zarrs::group::GroupBuilder::new().build(store.clone(), "/meta")?;
    group.async_store_metadata().await?;

    let dates = [
        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(2023, 1, 2).unwrap(),
        NaiveDate::from_ymd_opt(2023, 1, 3).unwrap(),
    ];

    // Convert to milliseconds since Unix epoch
    let date_data: Vec<i64> = dates
        .iter()
        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_millis())
        .collect();

    let date_array = ArrayBuilder::new(
        vec![3],
        vec![3],
        DataType::NumpyDateTime64 {
            unit: NumpyTimeUnit::Millisecond,
            scale_factor: 1.try_into().unwrap(),
        },
        FillValue::from(0i64),
    )
    .build(store.clone(), "/meta/date")?;

    date_array.async_store_metadata().await?;
    date_array
        .async_store_array_subset_elements(&ArraySubset::new_with_shape(vec![3]), &date_data)
        .await?;

    let collection_data = vec!["collection_a", "collection_b", "collection_c"];

    let collection_array =
        ArrayBuilder::new(vec![3], vec![3], DataType::String, FillValue::from(""))
            .build(store.clone(), "/meta/collection")?;

    collection_array.async_store_metadata().await?;
    collection_array
        .async_store_array_subset_elements(&ArraySubset::new_with_shape(vec![3]), &collection_data)
        .await?;

    // Create bbox array - variable length bytes (WKB format)
    // Boxes: [-10,-10,10,10], [-20,-20,20,20], [-30,-30,30,30]
    use geo::Polygon;
    use geo::Rect;
    use wkb::writer::{WriteOptions, write_polygon};

    let boxes = vec![
        Rect::new(
            geo::coord! { x: -10.0, y: -10.0 },
            geo::coord! { x: 10.0, y: 10.0 },
        ),
        Rect::new(
            geo::coord! { x: -20.0, y: -20.0 },
            geo::coord! { x: 20.0, y: 20.0 },
        ),
        Rect::new(
            geo::coord! { x: -30.0, y: -30.0 },
            geo::coord! { x: 30.0, y: 30.0 },
        ),
    ];

    let write_options = WriteOptions::default();
    let mut bbox_data: Vec<Vec<u8>> = Vec::new();
    for rect in &boxes {
        let polygon: Polygon = (*rect).into();
        let mut buffer = Vec::new();
        write_polygon(&mut buffer, &polygon, &write_options)?;
        bbox_data.push(buffer);
    }

    let bbox_array = ArrayBuilder::new(vec![3], vec![3], DataType::Bytes, FillValue::from(vec![]))
        .build(store.clone(), "/meta/bbox")?;

    bbox_array.async_store_metadata().await?;
    bbox_array
        .async_store_array_subset_elements(&ArraySubset::new_with_shape(vec![3]), &bbox_data)
        .await?;

    // Generate R-tree spatial index if requested
    if include_geoindex {
        use geo_index::rtree::{RTreeBuilder, sort::HilbertSort, util::f64_box_to_f32};

        // Create /indexes group
        let index_group = zarrs::group::GroupBuilder::new().build(store.clone(), "/indexes")?;
        index_group.async_store_metadata().await?;

        // Extract bounding boxes from the test data
        let mut xmin = Vec::new();
        let mut ymin = Vec::new();
        let mut xmax = Vec::new();
        let mut ymax = Vec::new();

        for rect in &boxes {
            xmin.push(rect.min().x);
            ymin.push(rect.min().y);
            xmax.push(rect.max().x);
            ymax.push(rect.max().y);
        }

        // Build R-tree index
        let mut rtree_builder = RTreeBuilder::<f32>::new(boxes.len() as u32);
        for i in 0..boxes.len() {
            let (min_x, min_y, max_x, max_y) = f64_box_to_f32(xmin[i], ymin[i], xmax[i], ymax[i]);
            rtree_builder.add(min_x, min_y, max_x, max_y);
        }
        let rtree = rtree_builder.finish::<HilbertSort>();
        let rtree_bytes = rtree.into_inner();

        // Store R-tree as a Zarr array
        let rtree_array = ArrayBuilder::new(
            vec![rtree_bytes.len() as u64],
            vec![rtree_bytes.len() as u64], // Single chunk
            DataType::UInt8,
            FillValue::from(0u8),
        )
        .build(store.clone(), "/indexes/bbox")?;

        rtree_array.async_store_metadata().await?;
        rtree_array
            .async_store_array_subset_elements(
                &ArraySubset::new_with_shape(vec![rtree_bytes.len() as u64]),
                rtree_bytes.as_slice(),
            )
            .await?;
    }

    Ok(())
}

/// Get a local Zarr store with test data
///
/// # Arguments
/// * `include_geoindex` - If true, generates an R-tree spatial index at /indexes/bbox
pub(crate) async fn get_local_zarr_store(include_geoindex: bool) -> LocalZarrStoreWrapper {
    let wrapper = LocalZarrStoreWrapper::new();
    let store = wrapper.get_store();
    generate_test_data_arrays(store, include_geoindex)
        .await
        .expect("Failed to generate test data arrays");
    wrapper
}

/// Get a local Icechunk store with test data
///
/// # Arguments
/// * `include_geoindex` - If true, generates an R-tree spatial index at /indexes/bbox
pub(crate) async fn get_local_icechunk_store(include_geoindex: bool) -> LocalIcechunkStoreWrapper {
    let wrapper = LocalIcechunkStoreWrapper::new().await;
    let store = wrapper.get_store();
    generate_test_data_arrays(store.clone(), include_geoindex)
        .await
        .expect("Failed to generate test data arrays");
    let _ = store
        .session()
        .write()
        .await
        .commit("test data", None)
        .await
        .unwrap();
    wrapper
}
