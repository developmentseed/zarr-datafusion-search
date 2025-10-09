// pub mod file_format;
// pub mod source;
#[cfg(test)]
pub mod testing;
// pub mod zarr_array;

// pub use file_format::{ZarrMetaFormat, ZarrMetaFormatFactory};

// #[cfg(test)]
// mod tests {
//     use std::env::current_dir;
//     use std::sync::Arc;

//     use datafusion::execution::SessionStateBuilder;
//     use datafusion::prelude::SessionContext;

//     use super::*;

//     #[tokio::test]
//     async fn test_geoparquet() {
//         let file_format = Arc::new(ZarrMetaFormatFactory::default());
//         let state = SessionStateBuilder::new()
//             .with_file_formats(vec![file_format])
//             .build();
//         let ctx = SessionContext::new_with_state(state).enable_url_table();

//         dbg!(current_dir().unwrap());
//         let df = ctx
//             .sql("SELECT * FROM 'data/zarr_store.zarr' as table")
//             .await
//             .unwrap();
//         df.show().await.unwrap();
//     }
// }
