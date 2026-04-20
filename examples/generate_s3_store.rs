/// CLI tool to populate an S3 Icechunk store with benchmark data.
///
/// Usage:
///   cargo run --example generate_s3_store -- <bucket> <prefix>
///
/// Examples:
///   cargo run --example generate_s3_store -- my-bucket bench/v1
#[path = "../benches/common/mod.rs"]
mod common;
use common::{ArraysToGenerate, generate_icechunk_store_s3};

use tokio::runtime::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("Usage: generate_s3_store <bucket> <prefix>");
        std::process::exit(1);
    }

    let bucket = args[1].clone();
    let prefix = args[2].clone();

    let arrays: Vec<ArraysToGenerate> = vec![
        ArraysToGenerate::Datetime,
        ArraysToGenerate::Bbox,
        ArraysToGenerate::BboxColumns,
        ArraysToGenerate::RtreeIndex,
    ];

    println!("Generating store at s3://{}/{}", bucket, prefix);

    let rt = Runtime::new()?;
    generate_icechunk_store_s3(&rt, bucket, prefix, &arrays)?;

    println!("Done.");
    Ok(())
}
