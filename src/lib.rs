#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]

pub mod error;
pub mod table_provider;
#[cfg(test)]
pub mod testing;
