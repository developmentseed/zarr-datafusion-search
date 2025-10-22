use datafusion::error::DataFusionError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ZarrDataFusionError {
    // Zarrs errors
    #[error("Zarrs array creation error: {0}")]
    ArrayCreateError(#[from] zarrs::array::ArrayCreateError),

    #[error("Zarrs filesystem create error: {0}")]
    FilesystemStoreCreateError(#[from] zarrs_filesystem::FilesystemStoreCreateError),

    #[error("Zarrs group create error: {0}")]
    GroupCreateError(#[from] zarrs::group::GroupCreateError),

    #[error("Zarrs error: {0}")]
    Zarrs(#[from] zarrs::array::ArrayError),

    // Other errors
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("{0}")]
    Custom(String),
}

impl From<ZarrDataFusionError> for DataFusionError {
    fn from(err: ZarrDataFusionError) -> Self {
        match err {
            ZarrDataFusionError::DataFusion(e) => e,
            _ => DataFusionError::External(Box::new(err)),
        }
    }
}

pub type ZarrDataFusionResult<T> = Result<T, ZarrDataFusionError>;
