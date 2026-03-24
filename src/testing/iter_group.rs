use crate::testing::utils::get_local_icechunk_store;
use crate::testing::utils::get_local_zarr_store;
use icechunk::{Repository, RepositoryConfig, repository::VersionInfo};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use zarrs::group::Group;
use zarrs_filesystem::FilesystemStore;
use zarrs_icechunk::AsyncIcechunkStore;

#[tokio::test]
async fn test_load_group() {
    let wrapper = get_local_zarr_store("data/load_group.zarr").await;
    let path = wrapper.get_store_path();

    {
        let storage = Arc::new(FilesystemStore::new(path).unwrap());
        let group = Group::open(storage.clone(), "/meta").unwrap();
        dbg!(group.path());
    }
}

#[tokio::test]
async fn test_load_group_icechunk() {
    let wrapper = get_local_icechunk_store("data/ice_group").await;
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
    let group = Group::async_open(store.clone(), "/meta").await.unwrap();
    dbg!(group.path());
}
