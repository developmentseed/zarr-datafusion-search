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
