use std::sync::Arc;
use zarrs::group::Group;
use zarrs_filesystem::FilesystemStore;

#[test]
fn test_load_group() {
    let storage = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

    let _group = Group::open(storage.clone(), "/meta").unwrap();
}
