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

    /// The builder or a helper was configured with invalid settings.
    #[error("Invalid configuration: {0}")]
    Config(String),

    /// This replica lost (or could not renew) its leader-election lease.
    ///
    /// Returned by [`ControllerBuilder::run`](crate::controller::ControllerBuilder::run).
    /// The controller was stopped immediately because another replica may
    /// already be reconciling. Exit the process with a non-zero status so the
    /// pod restarts and campaigns for leadership again.
    #[error("Leader election lost: {0}")]
    LeaderElectionLost(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl KubeGenericError {
    /// A short, bounded label that identifies the error category.
    ///
    /// Suitable as a Prometheus label value: the set of possible values is
    /// fixed and never contains object names, resource versions, or other
    /// unbounded data. For [`Kube`](Self::Kube) API errors the HTTP reason
    /// class is included (e.g. `kube_conflict`, `kube_not_found`).
    pub fn metric_label(&self) -> &'static str {
        match self {
            Self::Kube(kube::Error::Api(ae)) => match ae.code {
                400 => "kube_bad_request",
                401 => "kube_unauthorized",
                403 => "kube_forbidden",
                404 => "kube_not_found",
                409 => "kube_conflict",
                410 => "kube_gone",
                422 => "kube_invalid",
                429 => "kube_too_many_requests",
                500..=599 => "kube_server_error",
                _ => "kube_api_error",
            },
            Self::Kube(_) => "kube_client_error",
            Self::MissingMetadata(_) => "missing_metadata",
            Self::Serialization(_) => "serialization",
            Self::Io(_) => "io",
            Self::Config(_) => "config",
            Self::LeaderElectionLost(_) => "leader_election_lost",
            Self::Internal(_) => "internal",
        }
    }
}

pub type Result<T> = std::result::Result<T, KubeGenericError>;
