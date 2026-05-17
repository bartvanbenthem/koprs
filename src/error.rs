use thiserror::Error;

#[derive(Debug, Error)]
pub enum KubeGenericError {
    #[error("Kube API error: {0}")]
    Kube(#[from] kube::Error),

    #[error("Missing metadata field: {0}")]
    MissingMetadata(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, KubeGenericError>;
