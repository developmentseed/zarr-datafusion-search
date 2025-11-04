use icechunk::{Repository, RepositoryConfig, repository::VersionInfo};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use zarrs::group::Group;
use zarrs_filesystem::FilesystemStore;
use zarrs_icechunk::AsyncIcechunkStore;

#[test]
fn test_load_group() {
    let storage = Arc::new(FilesystemStore::new("data/zarr_store.zarr").unwrap());

    let group = Group::open(storage.clone(), "/meta").unwrap();
    dbg!(group.path());
}

#[tokio::test]
async fn test_load_group_icechunk() {
    let storage = icechunk::new_local_filesystem_storage(Path::new("data/icechunk"))
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
