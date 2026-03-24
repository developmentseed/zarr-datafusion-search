use futures::executor::block_on;
use icechunk::{ObjectStorage, Repository};
use object_store::local::LocalFileSystem;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zarrs::array::{ArrayBuilder, DataType, FillValue};
use zarrs::array_subset::ArraySubset;
use zarrs::metadata_ext::data_type::NumpyTimeUnit;
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_object_store::AsyncObjectStore;
use zarrs_storage::{
    AsyncReadableWritableListableStorageTraits, AsyncWritableStorageTraits, StorePrefix,
};

/// Helper function to cleanup a store and remove its directory
#[cfg(test)]
fn cleanup_store_and_directory(store: &dyn AsyncWritableStorageTraits, path: &Path) {
    // First, clear all data through the store interface
    let prefix = StorePrefix::new("").unwrap();
    let _ = block_on(store.erase_prefix(&prefix));

    // Then recursively remove the directory and all its contents
    if path.exists() {
        let _ = fs::remove_dir_all(path);
    }
}

pub(crate) struct LocalZarrStoreWrapper {
    store: Arc<AsyncObjectStore<LocalFileSystem>>,
    path: PathBuf,
}

// Note that this wrapper should use unique store names to avoid collisons with
// running concurrent binary test execution.
#[cfg(test)]
impl LocalZarrStoreWrapper {
    pub(crate) fn new(store_name: String) -> Self {
        if store_name.is_empty() {
            panic!("name for test zarr store cannot be empty!")
        }

        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(store_name);

        // Clean up any existing directory first (from failed tests or parallel execution)
        if p.exists() {
            let _ = fs::remove_dir_all(&p);
        }

        fs::create_dir(p.clone()).unwrap();
        let store = AsyncObjectStore::new(LocalFileSystem::new_with_prefix(p.clone()).unwrap());
        Self {
            store: Arc::new(store),
            path: p,
        }
    }

    pub(crate) fn get_store(&self) -> Arc<AsyncObjectStore<LocalFileSystem>> {
        self.store.clone()
    }

    pub(crate) fn get_store_path(&self) -> String {
        self.path.as_os_str().to_str().unwrap().into()
    }
}

// Include drop to remove store when it goes out of test scope
impl Drop for LocalZarrStoreWrapper {
    fn drop(&mut self) {
        cleanup_store_and_directory(self.store.as_ref(), &self.path);
    }
}

pub(crate) struct LocalIcechunkStoreWrapper {
    store: Arc<AsyncIcechunkStore>,
    path: PathBuf,
}

// Note that this wrapper should use unique store names to avoid collisons with
// running concurrent binary test execution.
#[cfg(test)]
impl LocalIcechunkStoreWrapper {
    pub(crate) async fn new(store_name: String) -> Self {
        if store_name.is_empty() {
            panic!("name for test icechunk repo cannot be empty!")
        }
        let p = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(store_name);
        fs::create_dir(p.clone()).unwrap();
        let repo = Repository::create(
            None,
            Arc::new(ObjectStorage::new_local_filesystem(&p).await.unwrap()),
            HashMap::new(),
        )
        .await
        .unwrap();
        let session = repo.writable_session("main").await.unwrap();
        Self {
            store: Arc::new(AsyncIcechunkStore::new(session)),
            path: p,
        }
    }

    pub(crate) fn get_store(&self) -> Arc<AsyncIcechunkStore> {
        self.store.clone()
    }

    pub(crate) fn get_store_path(&self) -> String {
        self.path.to_str().unwrap().into()
    }
}

// Include drop to remove store when it goes out of test scope
impl Drop for LocalIcechunkStoreWrapper {
    fn drop(&mut self) {
        cleanup_store_and_directory(self.store.as_ref(), &self.path);
    }
}

/// Creates three arrays in /meta group:
/// - date: datetime64[ms] array with dates [2023-01-01, 2023-01-02, 2023-01-03]
/// - collection: variable length UTF8 array with ["collection_a", "collection_b", "collection_c"]
/// - bbox: variable length bytes array with WKB-encoded boxes
#[cfg(test)]
pub(crate) async fn generate_test_data_arrays(
    store: Arc<dyn AsyncReadableWritableListableStorageTraits>,
) -> Result<(), Box<dyn std::error::Error>> {
    use chrono::NaiveDate;

    // Zarrs requires explicit group creation to
    // avoid implicit group issues.
    let group = zarrs::group::GroupBuilder::new().build(store.clone(), "/")?;
    group.async_store_metadata().await?;

    let group = zarrs::group::GroupBuilder::new().build(store.clone(), "/meta")?;
    group.async_store_metadata().await?;

    let dates = vec![
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
    for rect in boxes {
        let polygon: Polygon = rect.into();
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

    Ok(())
}

pub(crate) async fn get_local_zarr_store(dir_name: &str) -> LocalZarrStoreWrapper {
    let wrapper = LocalZarrStoreWrapper::new(dir_name.into());
    let store = wrapper.get_store();
    generate_test_data_arrays(store)
        .await
        .expect("Failed to generate test data arrays");
    wrapper
}

pub(crate) async fn get_local_icechunk_store(dir_name: &str) -> LocalIcechunkStoreWrapper {
    let wrapper = LocalIcechunkStoreWrapper::new(dir_name.into()).await;
    let store = wrapper.get_store();
    generate_test_data_arrays(store.clone())
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
