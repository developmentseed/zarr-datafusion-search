use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use icechunk::repository::VersionInfo;
use icechunk::{Repository, RepositoryConfig};
use tokio::runtime::Handle;

use crate::table_provider::ZarrTableProvider;
use crate::testing::utils::get_local_icechunk_store;

#[tokio::test(flavor = "multi_thread")]
async fn test_datafusion() {
    let ctx = SessionContext::new();

    let wrapper = get_local_icechunk_store().await;
    let path = wrapper.get_store_path();
    let storage = icechunk::new_local_filesystem_storage(Path::new(&path))
        .await
        .unwrap();
    let config = RepositoryConfig::default();
    let repo = Repository::open(Some(config), storage, HashMap::new())
        .await
        .unwrap();
    let version_info = VersionInfo::BranchTipRef("main".to_string());
    let icechunk_session = repo.readonly_session(&version_info).await.unwrap();

    let table_provider = Arc::new(
        ZarrTableProvider::new_icechunk(icechunk_session, Handle::current(), "/meta")
            .await
            .unwrap(),
    );
    ctx.register_table("zarr_data", table_provider).unwrap();

    let df = ctx.sql("SELECT * FROM zarr_data;").await.unwrap();
    df.show().await.unwrap();
}
