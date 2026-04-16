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

mod sentinel2_geometry;
use sentinel2_geometry::{generate_bbox_columns, generate_wkb_polygons};

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const SAMPLES_PER_DAY: usize = 10_000;
const MS_PER_DAY: i64 = 24 * 60 * 60 * 1000; // 86,400,000 milliseconds
const CHUNK_SIZE: u64 = 1_000_000; // 1M elements per chunk

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArraysToGenerate {
    Datetime,
    Bbox,
    BboxColumns,
    RtreeIndex,
}

// This generates:
// - 5,479 days (2010-01-01 to 2025-01-01, approximately 15 years)
// - 10,000 random timestamps per day
// - Total: 54,790,000 datetime64[ms] values
// - Chunks: 1,000,000 elements per chunk (approximately 10MB per chunk)
fn generate_icechunk_store(
    rt: &Runtime,
    storage: Arc<ObjectStorage>,
    arrays: &[ArraysToGenerate],
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

    if arrays.contains(&ArraysToGenerate::Datetime) {
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

    if arrays.contains(&ArraysToGenerate::Bbox) {
        let bbox_data = generate_wkb_polygons(array_shape[0] as usize);

        let bbox_blosc_codec: Arc<dyn zarrs::array::codec::BytesToBytesCodecTraits> = Arc::new(
            BloscCodec::new(
                BloscCompressor::LZ4,
                BloscCompressionLevel::try_from(3).unwrap(),
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

    if arrays.contains(&ArraysToGenerate::BboxColumns) {
        let (xmin, xmax, ymin, ymax) = generate_bbox_columns(array_shape[0] as usize);

        let f64_blosc_codec: Arc<dyn zarrs::array::codec::BytesToBytesCodecTraits> = Arc::new(
            BloscCodec::new(
                BloscCompressor::Zstd,
                BloscCompressionLevel::try_from(9).unwrap(),
                None,
                BloscShuffleMode::NoShuffle,
                None,
            )
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?,
        );

        // Create and store xmin array
        let xmin_array = ArrayBuilder::new(
            array_shape.clone(),
            chunk_shape.clone(),
            DataType::Float64,
            FillValue::from(0.0f64),
        )
        .bytes_to_bytes_codecs(vec![f64_blosc_codec.clone()])
        .build(store.clone(), "/meta/xmin")?;

        rt.block_on(xmin_array.async_store_metadata())?;
        rt.block_on(xmin_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(array_shape.clone()),
            &xmin,
        ))?;

        // Create and store xmax array
        let xmax_array = ArrayBuilder::new(
            array_shape.clone(),
            chunk_shape.clone(),
            DataType::Float64,
            FillValue::from(0.0f64),
        )
        .bytes_to_bytes_codecs(vec![f64_blosc_codec.clone()])
        .build(store.clone(), "/meta/xmax")?;

        rt.block_on(xmax_array.async_store_metadata())?;
        rt.block_on(xmax_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(array_shape.clone()),
            &xmax,
        ))?;

        // Create and store ymin array
        let ymin_array = ArrayBuilder::new(
            array_shape.clone(),
            chunk_shape.clone(),
            DataType::Float64,
            FillValue::from(0.0f64),
        )
        .bytes_to_bytes_codecs(vec![f64_blosc_codec.clone()])
        .build(store.clone(), "/meta/ymin")?;

        rt.block_on(ymin_array.async_store_metadata())?;
        rt.block_on(ymin_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(array_shape.clone()),
            &ymin,
        ))?;

        // Create and store ymax array
        let ymax_array = ArrayBuilder::new(
            array_shape.clone(),
            chunk_shape.clone(),
            DataType::Float64,
            FillValue::from(0.0f64),
        )
        .bytes_to_bytes_codecs(vec![f64_blosc_codec])
        .build(store.clone(), "/meta/ymax")?;

        rt.block_on(ymax_array.async_store_metadata())?;
        rt.block_on(ymax_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(array_shape.clone()),
            &ymax,
        ))?;
    }
    if arrays.contains(&ArraysToGenerate::RtreeIndex) {
        let index_group = zarrs::group::GroupBuilder::new().build(store.clone(), "/indexes")?;
        rt.block_on(index_group.async_store_metadata())?;

        let (xmin, xmax, ymin, ymax) = generate_bbox_columns(array_shape[0] as usize);
        let start = std::time::Instant::now();

        use geo_index::rtree::{RTreeBuilder, sort::HilbertSort, util::f64_box_to_f32};

        // Use f32 instead of f64 to reduce index size by ~50%, using f64_box_to_f32
        // to ensure each f32 box is no smaller than the original f64 box (prevents
        // false negatives during spatial filtering due to precision loss).
        let mut rtree_builder = RTreeBuilder::<f32>::new(xmin.len() as u32);
        for i in 0..xmin.len() {
            let (min_x, min_y, max_x, max_y) =
                f64_box_to_f32(xmin[i], ymin[i], xmax[i], ymax[i]);
            rtree_builder.add(min_x, min_y, max_x, max_y);
        }
        let rtree = rtree_builder.finish::<HilbertSort>();
        let rtree_bytes = rtree.into_inner();

        println!(
            "Built R-tree index in {:?} - {} items, {} bytes ({:.2} MB)",
            start.elapsed(),
            xmin.len(),
            rtree_bytes.len(),
            rtree_bytes.len() as f64 / 1_048_576.0
        );

        let rtree_blosc_codec: Arc<dyn zarrs::array::codec::BytesToBytesCodecTraits> = Arc::new(
            BloscCodec::new(
                BloscCompressor::LZ4,
                BloscCompressionLevel::try_from(3).unwrap(),
                None,
                BloscShuffleMode::NoShuffle,
                None,
            )
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?,
        );

        let rtree_array = ArrayBuilder::new(
            vec![rtree_bytes.len() as u64],
            vec![rtree_bytes.len() as u64], // Single chunk
            DataType::UInt8,
            FillValue::from(0u8),
        )
        .bytes_to_bytes_codecs(vec![rtree_blosc_codec])
        .build(store.clone(), "/indexes/bbox")?;

        rt.block_on(rtree_array.async_store_metadata())?;
        rt.block_on(rtree_array.async_store_array_subset_elements(
            &ArraySubset::new_with_shape(vec![rtree_bytes.len() as u64]),
            rtree_bytes.as_slice(),
        ))?;

        // Read back the compressed chunk to show actual storage size
        use zarrs::storage::AsyncReadableStorageTraits;
        let chunk_key = rtree_array.chunk_key(&[0]);
        let compressed_chunk = rt.block_on(async { store.get(&chunk_key).await })?;
        if let Some(chunk_bytes) = compressed_chunk {
            let compression_ratio = rtree_bytes.len() as f64 / chunk_bytes.len() as f64;
            println!(
                "R-tree index stored in Zarr at /indexes/bbox - uncompressed: {:.2} MB, compressed: {:.2} MB (ratio: {:.2}x)",
                rtree_bytes.len() as f64 / 1_048_576.0,
                chunk_bytes.len() as f64 / 1_048_576.0,
                compression_ratio
            );
        } else {
            println!("R-tree index stored in Zarr at /indexes/bbox");
        }
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
    arrays: &[ArraysToGenerate],
) -> Result<(Session, TempDir), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let storage = rt.block_on(ObjectStorage::new_local_filesystem(temp_dir.path()))?;
    let session = generate_icechunk_store(rt, Arc::new(storage), arrays)?;
    Ok((session, temp_dir))
}

pub fn generate_icechunk_store_s3(
    rt: &Runtime,
    bucket: String,
    prefix: String,
    arrays: &[ArraysToGenerate],
) -> Result<Session, Box<dyn std::error::Error>> {
    let storage = rt.block_on(ObjectStorage::new_s3(
        bucket,
        Some(prefix),
        None, // credentials - uses default AWS credential chain
        None, // config - uses default S3 options
    ))?;
    let session = generate_icechunk_store(rt, Arc::new(storage), arrays)?;
    Ok(session)
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
            let batches = df.collect().await.unwrap();
            let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();
            println!("Query returned {} rows", row_count);
            batches
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
    date < CAST('2025-01-01' AS DATE) \
    and date > CAST('2024-12-25' AS DATE)\
";

pub static BBOX_SQL: &str = "\
    SELECT date FROM zarr_data \
    WHERE ST_Intersects(bbox, ST_GeomFromText('POLYGON((0 -7, 0 7, 5 7, 5 -7, 0 -7))')) \
";

pub static BBOX_COLUMNS_SQL: &str = "\
    SELECT xmin, xmax, ymin, ymax FROM zarr_data \
    WHERE xmin <= 5 AND xmax >= 0 AND ymin <= 7 AND ymax >= -7 \
";
