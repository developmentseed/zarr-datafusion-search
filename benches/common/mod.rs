// Common benchmark utilities shared across datetime benchmarks
// Functions/constants here are used by datetime_local.rs and datetime_s3.rs
#![allow(dead_code)]

use bytesize::ByteSize;
use chrono::NaiveDate;
use criterion::{Criterion, SamplingMode};
use datafusion::prelude::SessionContext;
use icechunk::session::Session;
use icechunk::{ObjectStorage, Repository, repository::VersionInfo};
use rand::Rng;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;
use zarrs::array::codec::{BloscCodec, BloscCompressionLevel, BloscCompressor, BloscShuffleMode};
use zarrs::array::{ArrayBuilder, DataType, FillValue};
use zarrs::array_subset::ArraySubset;
use zarrs::metadata_ext::data_type::NumpyTimeUnit;
use zarrs_icechunk::AsyncIcechunkStore;

mod s2_geometry;
use s2_geometry::generate_s2_wkb_polygons;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const SAMPLES_PER_DAY: usize = 10_000;
const MS_PER_DAY: i64 = 24 * 60 * 60 * 1000; // 86,400,000 milliseconds
const CHUNK_SIZE: u64 = 1_000_000; // 1M elements per chunk

#[derive(Debug, Clone, Copy)]
pub enum ArraysToGenerate {
    DatetimeOnly,
    BboxOnly,
    Both,
}

// This generates:
// - 5,479 days (2010-01-01 to 2025-01-01, approximately 15 years)
// - 10,000 random timestamps per day
// - Total: 54,790,000 datetime64[ms] values
// - Chunks: 1,000,000 elements per chunk (approximately 10MB per chunk)
fn generate_icechunk_store(
    rt: &Runtime,
    storage: Arc<ObjectStorage>,
    arrays: ArraysToGenerate,
) -> Result<Session, Box<dyn std::error::Error>> {
    let _guard = rt.enter();

    let repo = rt.block_on(Repository::create(None, storage, HashMap::new()))?;
    let session = rt.block_on(repo.writable_session("main")).unwrap();
    let store = Arc::new(AsyncIcechunkStore::new(session.clone()));

    let root_group = zarrs::group::GroupBuilder::new().build(store.clone(), "/")?;
    rt.block_on(root_group.async_store_metadata())?;

    let meta_group = zarrs::group::GroupBuilder::new().build(store.clone(), "/meta")?;
    rt.block_on(meta_group.async_store_metadata())?;

    let start_date = NaiveDate::from_ymd_opt(2010, 1, 1).unwrap();
    let end_date = NaiveDate::from_ymd_opt(2025, 1, 1).unwrap();
    let num_days = (end_date - start_date).num_days() as usize;

    let mut date_data: Vec<i64> = Vec::with_capacity(num_days * SAMPLES_PER_DAY);
    let mut rng = rand::thread_rng();

    for day_offset in 0..num_days {
        let date = start_date + chrono::Duration::days(day_offset as i64);
        let day_ms = date
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();

        // Generate 10,000 random millisecond offsets for this day
        for _ in 0..SAMPLES_PER_DAY {
            let random_ms_offset = rng.gen_range(0..MS_PER_DAY);
            date_data.push(day_ms + random_ms_offset);
        }
    }

    let array_shape = vec![date_data.len() as u64];
    let chunk_shape = vec![CHUNK_SIZE];

    if matches!(
        arrays,
        ArraysToGenerate::DatetimeOnly | ArraysToGenerate::Both
    ) {
        let date_blosc_codec: Arc<dyn zarrs::array::codec::BytesToBytesCodecTraits> = Arc::new(
            BloscCodec::new(
                BloscCompressor::Zstd,
                BloscCompressionLevel::try_from(9).unwrap(),
                None,
                BloscShuffleMode::NoShuffle,
                None,
            )
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?,
        );

        let date_array = ArrayBuilder::new(
            array_shape.clone(),
            chunk_shape.clone(),
            DataType::NumpyDateTime64 {
                unit: NumpyTimeUnit::Millisecond,
                scale_factor: 1.try_into().unwrap(),
            },
            FillValue::from(0i64),
        )
        .bytes_to_bytes_codecs(vec![date_blosc_codec])
        .build(store.clone(), "/meta/date")?;

        rt.block_on(date_array.async_store_metadata())?;

        rt.block_on(date_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(array_shape.clone()),
            &date_data,
        ))?;
    }

    if matches!(arrays, ArraysToGenerate::BboxOnly | ArraysToGenerate::Both) {
        let bbox_data = generate_s2_wkb_polygons(array_shape[0] as usize);

        let bbox_blosc_codec: Arc<dyn zarrs::array::codec::BytesToBytesCodecTraits> = Arc::new(
            BloscCodec::new(
                BloscCompressor::Zstd,
                BloscCompressionLevel::try_from(9).unwrap(),
                None, // no typesize for variable-length data
                BloscShuffleMode::NoShuffle,
                None,
            )
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?,
        );

        let bbox_array = ArrayBuilder::new(
            array_shape.clone(),
            chunk_shape.clone(),
            DataType::Bytes,
            FillValue::from(vec![]),
        )
        .bytes_to_bytes_codecs(vec![bbox_blosc_codec])
        .build(store.clone(), "/meta/bbox")?;

        rt.block_on(bbox_array.async_store_metadata())?;

        rt.block_on(bbox_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(array_shape.clone()),
            &bbox_data,
        ))?;
    }

    rt.block_on(async {
        store
            .session()
            .write()
            .await
            .commit("Large dataset with millions of values", None)
            .await
    })?;

    // Open a readonly session to read the data back
    let readonly_session = rt
        .block_on(repo.readonly_session(&VersionInfo::BranchTipRef("main".to_string())))
        .unwrap();
    Ok(readonly_session)
}

pub fn generate_icechunk_store_local(
    rt: &Runtime,
    arrays: ArraysToGenerate,
) -> Result<(Session, TempDir), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let storage = rt.block_on(ObjectStorage::new_local_filesystem(temp_dir.path()))?;
    let session = generate_icechunk_store(rt, Arc::new(storage), arrays)?;
    Ok((session, temp_dir))
}

pub fn run_bench(
    c: &mut Criterion,
    rt: &Runtime,
    ctx: &SessionContext,
    group_name: &str,
    bench_name: &str,
    sql: &str,
) {
    // Run criterion benchmarks
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10); // Minimum is 10 samples
    group.sampling_mode(SamplingMode::Flat); // Run each benchmark exactly once per sample
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(2));

    group.bench_function(bench_name, |b| {
        b.to_async(rt).iter(|| async {
            let df = ctx.sql(black_box(sql)).await.unwrap();
            df.collect().await.unwrap()
        });
    });

    group.finish();
}

pub fn run_memory_profile(rt: &Runtime, ctx: &SessionContext, sql: &str) {
    // Run dhat memory benchmark in closure to avoid criterion profiing
    {
        let _profiler = dhat::Profiler::builder()
            .trim_backtraces(None) // minimal output
            .build();
        rt.block_on(async {
            let df = ctx.sql(sql).await.unwrap();
            let _results = df.collect().await.unwrap();
        });
        let stats = dhat::HeapStats::get();
        println!("peak heap: {} bytes", ByteSize(stats.max_bytes as u64));
    }
}

pub static DATETIME_SQL: &str = "\
    SELECT date FROM zarr_data WHERE \
    date < CAST('2025-10-11' AS DATE) \
    and date > CAST('2025-09-01' AS DATE)\
";

pub static BBOX_SQL: &str = "\
    SELECT bbox FROM zarr_data \
    WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((0 -7, 0 7, 5 7, 5 -7, 0 -7))')) \
";
