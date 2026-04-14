mod common;

use common::{ArraysToGenerate, generate_icechunk_store_local};
use criterion::{Criterion, SamplingMode, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::runtime::Runtime;
use zarrs::array::Array;
use zarrs_icechunk::AsyncIcechunkStore;

fn bbox_zarrs_raw_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (session, _temp_dir) = generate_icechunk_store_local(&rt, &[ArraysToGenerate::Bbox]).unwrap();
    let store = Arc::new(AsyncIcechunkStore::new(session));

    // Criterion benchmark
    let mut group = c.benchmark_group("bbox_zarrs_raw");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(2));

    group.bench_function("fetch_all_bbox_chunks", |b| {
        b.to_async(&rt).iter(|| async {
            let bbox_array = Array::async_open(store.clone(), "/meta/bbox")
                .await
                .unwrap();

            let chunk_grid_shape = bbox_array.chunk_grid_shape();
            let num_chunks: u64 = chunk_grid_shape.iter().product();

            let mut total_rows = 0usize;
            let max_rows = 3_000_000;

            for chunk_idx in 0..num_chunks {
                if total_rows >= max_rows {
                    break;
                }

                let chunk_indices = vec![chunk_idx];
                let subset = bbox_array.chunk_subset_bounded(&chunk_indices).unwrap();

                let data: Vec<Vec<u8>> = bbox_array
                    .async_retrieve_array_subset_elements(&subset)
                    .await
                    .unwrap();

                total_rows += data.len();
            }

            total_rows.min(max_rows)
        });
    });

    group.finish();
}

criterion_group!(benches, bbox_zarrs_raw_bench);
criterion_main!(benches);
