use arrow_array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use geoarrow_array::GeoArrowArray;
use geoarrow_array::array::WktArray;
use geoarrow_schema::Crs;
use std::sync::Arc;
use zarrs::array::Array;
use zarrs::array_subset::ArraySubset;
use zarrs_filesystem::FilesystemStore;

#[test]
fn test_load_zarrs_into_arrow_record_batch() {
    // Create a filesystem store pointing to the zarr store
    let store = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

    // Load collection array (strings)
    let collection_array = Array::open(store.clone(), "/meta/collection").unwrap();
    let collection_subset = ArraySubset::new_with_shape(collection_array.shape().to_vec());
    let collection_data: Vec<String> = collection_array
        .retrieve_array_subset_elements(&collection_subset)
        .unwrap();

    // Load date array (datetime64[ms])
    let date_array = Array::open(store.clone(), "/meta/date").unwrap();
    let date_subset = ArraySubset::new_with_shape(date_array.shape().to_vec());
    let date_data: Vec<i64> = date_array
        .retrieve_array_subset_elements(&date_subset)
        .unwrap();

    // Load bbox array (strings representing WKT geometries)
    let bbox_array = Array::open(store.clone(), "/meta/bbox").unwrap();
    let bbox_subset = ArraySubset::new_with_shape(bbox_array.shape().to_vec());
    let bbox_data: Vec<String> = bbox_array
        .retrieve_array_subset_elements(&bbox_subset)
        .unwrap();

    // Create Arrow arrays from the loaded data
    let collection_arrow: ArrayRef = Arc::new(StringArray::from(collection_data.clone()));
    let date_arrow: ArrayRef = Arc::new(TimestampMillisecondArray::from(date_data.clone()));
    let wkt_crs = Crs::from_authority_code("EPSG:4326".to_string());
    let wkt_metadata = Arc::new(geoarrow_schema::Metadata::new(wkt_crs, None));
    let wkt_arrow = WktArray::new(bbox_data.clone().into(), wkt_metadata);

    // Define the schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("collection", DataType::Utf8, false),
        Field::new(
            "date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        wkt_arrow.data_type().to_field("bbox", false),
    ]));

    // Create the RecordBatch
    let record_batch = RecordBatch::try_new(
        schema.clone(),
        vec![collection_arrow, date_arrow, wkt_arrow.into_array_ref()],
    )
    .unwrap();

    // Print the record batch
    println!("Created Arrow RecordBatch from Zarr arrays:");
    println!("  Schema: {:?}", record_batch.schema());
    println!("  Num rows: {}", record_batch.num_rows());
    println!("  Num columns: {}", record_batch.num_columns());
    println!("\nData:");
    for i in 0..record_batch.num_rows() {
        println!(
            "  Row {}: collection='{}', date={}, bbox={}",
            i, collection_data[i], date_data[i], bbox_data[i]
        );
    }

    // Assertions
    assert_eq!(record_batch.num_rows(), 3);
    assert_eq!(record_batch.num_columns(), 3);
    assert_eq!(record_batch.schema().fields().len(), 3);

    // Verify the data
    let collection_col = record_batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(collection_col.value(0), "collection_a");
    assert_eq!(collection_col.value(1), "collection_b");
    assert_eq!(collection_col.value(2), "collection_c");

    let date_col = record_batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert_eq!(date_col.value(0), 1672531200000);
    assert_eq!(date_col.value(1), 1672617600000);
    assert_eq!(date_col.value(2), 1672704000000);
}
