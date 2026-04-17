mod common;

use common::{
    ArraysToGenerate, BBOX_SQL, generate_icechunk_store_local, run_bench, run_memory_profile,
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use std::sync::Arc;
use tokio::runtime::Runtime;
use zarr_datafusion_search::table_provider::ZarrTableProvider;

fn bbox_bench_local(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (session, _temp_dir) = generate_icechunk_store_local(
        &rt,
        &[
            ArraysToGenerate::Datetime,
            ArraysToGenerate::Bbox,
            ArraysToGenerate::RtreeIndex,
        ],
    )
    .unwrap();
    let table_provider = Arc::new(
        rt.block_on(ZarrTableProvider::new_icechunk(session, "/meta"))
            .unwrap(),
    );

    let ctx = SessionContext::new();
    geodatafusion::register(&ctx);
    ctx.register_table("zarr_data", table_provider).unwrap();

    run_memory_profile(&rt, &ctx, BBOX_SQL);
    run_bench(
        c,
        &rt,
        &ctx,
        "bbox_bench_local",
        "bbox_bench_local",
        BBOX_SQL,
    );
}

criterion_group!(benches_local, bbox_bench_local);
criterion_main!(benches_local);
