mod common;

use common::{BBOX_SQL, run_bench};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use icechunk::{ObjectStorage, Repository, repository::VersionInfo};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;
use zarr_datafusion_search::table_provider::ZarrTableProvider;

fn bbox_bench_s3(c: &mut Criterion) {
    let bucket = "zarr-datafusion-search".to_string();
    let prefix = "".to_string();

    let rt = Runtime::new().unwrap();
    let storage = rt
        .block_on(ObjectStorage::new_s3(
            bucket,
            Some(prefix),
            None, // credentials - uses default AWS credential chain
            None, // config - uses default S3 options
        ))
        .unwrap();
    let repo = rt
        .block_on(Repository::open_or_create(
            None,
            Arc::new(storage),
            HashMap::new(),
        ))
        .unwrap();
    let session = rt
        .block_on(repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())))
        .unwrap();

    let table_provider = Arc::new(
        rt.block_on(ZarrTableProvider::new_icechunk(session, "/meta"))
            .unwrap(),
    );

    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx.register_table("zarr_data", table_provider).unwrap();
    run_bench(c, &rt, &ctx, "bbox_bench_s3", "bbox_bench_s3", BBOX_SQL);
}

criterion_group!(benches_s3, bbox_bench_s3);
criterion_main!(benches_s3);
