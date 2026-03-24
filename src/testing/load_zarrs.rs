use crate::testing::utils::get_local_icechunk_store;
use crate::testing::utils::get_local_zarr_store;
use icechunk::{Repository, RepositoryConfig, repository::VersionInfo};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs_filesystem::FilesystemStore;
use zarrs_icechunk::AsyncIcechunkStore;

#[tokio::test]
async fn test_load_collection_array() {
    let wrapper = get_local_zarr_store("data/collection.zarr").await;
    let path = wrapper.get_store_path();

    {
        let store = Arc::new(FilesystemStore::new(path).unwrap());

        // Open the collection array from the /meta/collection path
        let collection_array = Array::open(store, "/meta/collection").unwrap();

        // Print array metadata
        println!("Array shape: {:?}", collection_array.shape());
        println!("Data type: {:?}", collection_array.data_type());

        // Create array subset for the entire array (shape is [3])
        let array_subset = ArraySubset::new_with_shape(collection_array.shape().to_vec());

        // Read the entire array as strings
        let data: Vec<String> = collection_array
            .retrieve_array_subset_elements(&array_subset)
            .unwrap();

        println!("Collection array contents:");
        for (i, item) in data.iter().enumerate() {
            println!("  [{}]: {}", i, item);
        }

        // Basic assertions
        assert!(!data.is_empty(), "Collection array should not be empty");
        assert_eq!(
            collection_array.shape(),
            &[3],
            "Collection array should have 3 elements"
        );
    }
}

#[tokio::test]
async fn test_load_collection_array_icechunk() {
    let wrapper = get_local_icechunk_store("data/ice_array").await;
    let path = wrapper.get_store_path();
    let storage = icechunk::new_local_filesystem_storage(Path::new(&path))
        .await
        .unwrap();
    let config = RepositoryConfig::default();
    let repo = Repository::open(Some(config), storage, HashMap::new())
        .await
        .unwrap();
    let version_info = VersionInfo::BranchTipRef("main".to_string());
    let session = repo.readonly_session(&version_info).await.unwrap();
    let store = Arc::new(AsyncIcechunkStore::new(session));

    // Open the collection array from the /meta/collection path
    let collection_array = Array::async_open(store, "/meta/collection").await.unwrap();

    // Print array metadata
    println!("Array shape: {:?}", collection_array.shape());
    println!("Data type: {:?}", collection_array.data_type());

    // Create array subset for the entire array (shape is [3])
    let array_subset = ArraySubset::new_with_shape(collection_array.shape().to_vec());

    //// Read the entire array as strings
    let data: Vec<String> = collection_array
        .async_retrieve_array_subset_elements(&array_subset)
        .await
        .unwrap();

    println!("Collection array contents:");
    for (i, item) in data.iter().enumerate() {
        println!("  [{}]: {}", i, item);
    }

    //// Basic assertions
    assert!(!data.is_empty(), "Collection array should not be empty");
    assert_eq!(
        collection_array.shape(),
        &[3],
        "Collection array should have 3 elements"
    );
}

#[tokio::test]
async fn test_load_date_array() {
    let wrapper = get_local_zarr_store("data/date.zarr").await;
    let path = wrapper.get_store_path();

    let store = Arc::new(FilesystemStore::new(path).unwrap());

    // Open the date array from the /meta/date path
    let date_array = Array::open(store, "/meta/date").unwrap();

    // Print array metadata
    println!("Array shape: {:?}", date_array.shape());
    println!("Data type: {:?}", date_array.data_type());

    // Create array subset for the entire array (shape is [3])
    let array_subset = ArraySubset::new_with_shape(date_array.shape().to_vec());

    // Read the entire array as i64 milliseconds (datetime64[ms])
    let data: Vec<i64> = date_array
        .retrieve_array_subset_elements(&array_subset)
        .unwrap();

    println!("Date array contents (milliseconds since epoch):");
    for (i, ms) in data.iter().enumerate() {
        println!("  [{}]: {} ms", i, ms);
    }

    // Basic assertions
    assert!(!data.is_empty(), "Date array should not be empty");
    assert_eq!(
        date_array.shape(),
        &[3],
        "Date array should have 3 elements"
    );
}

#[tokio::test]
async fn test_load_date_array_icechunk() {
    let wrapper = get_local_icechunk_store("data/ice_date_array").await;
    let path = wrapper.get_store_path();
    let storage = icechunk::new_local_filesystem_storage(Path::new(&path))
        .await
        .unwrap();
    let config = RepositoryConfig::default();
    let repo = Repository::open(Some(config), storage, HashMap::new())
        .await
        .unwrap();
    let version_info = VersionInfo::BranchTipRef("main".to_string());
    let session = repo.readonly_session(&version_info).await.unwrap();
    let store = Arc::new(AsyncIcechunkStore::new(session));

    // Open the date array from the /meta/date path
    let date_array = Array::async_open(store, "/meta/date").await.unwrap();

    // Print array metadata
    println!("Array shape: {:?}", date_array.shape());
    println!("Data type: {:?}", date_array.data_type());

    // Create array subset for the entire array (shape is [3])
    let array_subset = ArraySubset::new_with_shape(date_array.shape().to_vec());

    // Read the entire array as i64 milliseconds (datetime64[ms])
    let data: Vec<i64> = date_array
        .async_retrieve_array_subset_elements(&array_subset)
        .await
        .unwrap();

    println!("Date array contents (milliseconds since epoch):");
    for (i, ms) in data.iter().enumerate() {
        println!("  [{}]: {} ms", i, ms);
    }

    // Basic assertions
    assert!(!data.is_empty(), "Date array should not be empty");
    assert_eq!(
        date_array.shape(),
        &[3],
        "Date array should have 3 elements"
    );
}

#[tokio::test]
async fn test_load_bbox_array() {
    let wrapper = get_local_zarr_store("data/bbox.zarr").await;
    let path = wrapper.get_store_path();

    {
        let store = Arc::new(FilesystemStore::new(path).unwrap());

        let bbox_array = Array::open(store.clone(), "/meta/bbox").unwrap();

        println!("HII");
        dbg!(bbox_array.data_type());
    }
}
