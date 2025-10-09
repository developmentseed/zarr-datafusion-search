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

#[test]
fn test_load_bbox_array() {
    // Create a filesystem store pointing to the zarr store
    let store = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

    let bbox_array = Array::open(store.clone(), "/meta/bbox").unwrap();

    println!("HII");
    dbg!(bbox_array.data_type());

    // let array_subset = ArraySubset::new_with_shape(bbox_array.shape().to_vec());
    // let data: Vec<Vec<u8>> = bbox_array
    //     .retrieve_array_subset_elements(&array_subset)
    //     .unwrap();
    // dbg!(data);

    // // Note: The bbox array uses "variable_length_bytes" data type which is not yet
    // // fully supported in zarrs 0.21.2. This test demonstrates reading the raw chunk data.

    // println!("Reading bbox array chunk data from meta/bbox/c/0");

    // // Read the raw chunk data directly from storage
    // let chunk_key = StoreKey::new("meta/bbox/c/0").unwrap();
    // let chunk_data = store.get(&chunk_key).unwrap().unwrap();
    // let chunk_bytes: Vec<u8> = chunk_data.to_vec();

    // println!("Bbox chunk data:");
    // println!("  Chunk key: {}", chunk_key);
    // println!("  Raw bytes length: {} bytes", chunk_bytes.len());
    // println!(
    //     "  First 64 bytes (or all if less): {:?}",
    //     &chunk_bytes[..chunk_bytes.len().min(64)]
    // );

    // // Basic assertions
    // assert!(
    //     !chunk_bytes.is_empty(),
    //     "Bbox chunk data should not be empty"
    // );

    // // The chunk contains compressed data (zstd) for 3 variable-length byte arrays
    // println!("\nNote: This is the compressed chunk data. To properly decode:");
    // println!("  1. Decompress with zstd");
    // println!("  2. Decode vlen-bytes format (offsets + data)");
}
