use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs_filesystem::FilesystemStore;

#[test]
fn test_load_collection_array() {
    // Create a filesystem store pointing to the zarr store
    let store = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

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

#[test]
fn test_load_date_array() {
    // Create a filesystem store pointing to the zarr store
    let store = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

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
