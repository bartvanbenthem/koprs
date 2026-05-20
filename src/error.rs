use thiserror::Error;

#[derive(Debug, Error)]
pub enum KubeGenericError {
    #[error("Kube API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Missing metadata field: {0}")]
    MissingMetadata(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, KubeGenericError>;
