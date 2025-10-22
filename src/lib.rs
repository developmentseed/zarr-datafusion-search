#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

// Silence unused dependency lint; we override the version to include
// https://github.com/zarrs/zarrs/pull/289
use zarrs_metadata as _;

pub mod error;
pub mod schema;
pub mod table_provider;
#[cfg(test)]
pub mod testing;
