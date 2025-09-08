//! Load Zarr array as Arrow array.

// fn load

#[cfg(test)]
mod tests {
    use std::env::current_dir;
    use std::sync::Arc;

    use arrow_schema::{DataType as ArrowDataType, Field, Schema, TimeUnit};
    use arrow_zarr::table::{ZarrTable, ZarrTableFactory};
    use datafusion::catalog::TableProvider;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;
    use object_store::local::LocalFileSystem;
    use zarrs::group::Group;
    use zarrs_filesystem::FilesystemStore;
    use zarrs_object_store::AsyncObjectStore;
    use zarrs_storage::{
        AsyncReadableListableStorage, AsyncReadableListableStorageTraits, ReadableListableStorage,
    };

    use super::*;

    fn create_store_sync() -> ReadableListableStorage {
        let dir = current_dir().unwrap();
        let store = FilesystemStore::new(dir.join("data/zarr_store.zarr")).unwrap();
        Arc::new(store)
        // ReadableListableStorage
        // AsyncObjectStore::new(
        //     LocalFileSystem::new_with_prefix(dir.join("data/zarr_store.zarr/meta")).unwrap(),
        // )
    }

    fn create_store_async() -> Arc<dyn AsyncReadableListableStorageTraits + Send + Unpin> {
        let dir = current_dir().unwrap();
        // let store = FilesystemStore::new(dir.join("data/zarr_store.zarr")).unwrap();
        Arc::new(AsyncObjectStore::new(
            LocalFileSystem::new_with_prefix(dir.join("data/zarr_store.zarr/meta")).unwrap(),
        ))
    }

    #[test]
    fn read_binary() {
        let store = create_store_sync();
        let group = Group::open(store, "meta").unwrap();
        dbg!(group.attributes());

        // let array = store
        //     .get("0.0.0")
        //     .unwrap()
        //     .bytes()
        //     .unwrap()
        //     .collect::<Result<Vec<_>, _>>()
        //     .unwrap()
        //     .concat();
        // assert_eq!(array.len(), 8 * 8 * 4);
    }

    #[tokio::test]
    async fn test_arrow_zarr() {
        let store = create_store_async();
        let schema = Arc::new(Schema::new(vec![
            Field::new("bbox", ArrowDataType::Binary, false),
            // Field::new("collection", ArrowDataType::Utf8, false),
            // Field::new(
            //     "date",
            //     ArrowDataType::Timestamp(TimeUnit::Second, None),
            //     false,
            // ),
        ]));

        dbg!("creating ZarrTable");
        let table_provider = ZarrTable::new(schema, store);
        dbg!("finished creating ZarrTable");
        let state = SessionStateBuilder::new().build();
        let session = SessionContext::new();

        dbg!("Creating scan");
        let scan = table_provider
            .scan(&state, None, &Vec::new(), None)
            .await
            .unwrap();
        dbg!("Finished Creating scan");
        let records: Vec<_> = scan
            .execute(0, session.task_ctx())
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        // let mut state = SessionStateBuilder::new().build();
        // let table_path = wrapper.get_store_path();
        // state
        //     .table_factories_mut()
        //     .insert("ZARR_LOCAL_FOLDER".into(), Arc::new(ZarrTableFactory {}));

        // // create a table with 2 explicitly selected columns
        // let query = format!(
        //     "CREATE EXTERNAL TABLE zarr_table_partial(lat double, lon double) STORED AS ZARR_LOCAL_FOLDER LOCATION '{}'",
        //     table_path,
        // );

        // let session = SessionContext::new_with_state(state.clone());
        // session.sql(&query).await.unwrap();

        // // both columns are 1d coordinates. This should get resolved to
        // // all combinations of lat with lon (8 lats, 8 lons -> 64 rows).
        // let query = "SELECT lat, lon FROM zarr_table_partial";
        // let df = session.sql(query).await.unwrap();
        // let batches = df.collect().await.unwrap();

        // let schema = batches[0].schema();
        // let batch = concat_batches(&schema, &batches).unwrap();
        // assert_eq!(batch.num_columns(), 2);
        // assert_eq!(batch.num_rows(), 64);

        // // create a table, with 3 columns, lat, lon and data.
        // let query = format!(
        //     "CREATE EXTERNAL TABLE zarr_table STORED AS ZARR_LOCAL_FOLDER LOCATION '{}'",
        //     table_path,
        // );
    }
}
