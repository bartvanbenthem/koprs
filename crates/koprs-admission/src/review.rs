//! Admission review types.
//!
//! [`AdmissionRequest`] is the typed representation of the incoming webhook
//! payload that [`Validator`][crate::webhook::Validator] implementations
//! receive. [`ValidationResponse`] is what they return.
//!
//! [`Operation`] distinguishes the Kubernetes API verb that triggered the
//! admission request (CREATE, UPDATE, DELETE, CONNECT).

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::{AdmissionError, Result};

// ---------------------------------------------------------------------------
// Operation
// ---------------------------------------------------------------------------

/// The Kubernetes API operation that triggered the admission request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// The resource is being created for the first time.
    Create,
    /// An existing resource is being modified.
    Update,
    /// The resource is being deleted.
    Delete,
    /// A CONNECT request (used for proxying).
    Connect,
    /// An operation string not recognised by this version of the library.
    Unknown(String),
}

impl Operation {
    pub(crate) fn from_str(s: &str) -> Self {
        match s {
            "CREATE" => Self::Create,
            "UPDATE" => Self::Update,
            "DELETE" => Self::Delete,
            "CONNECT" => Self::Connect,
            other => Self::Unknown(other.to_string()),
        }
    }
}

// ---------------------------------------------------------------------------
// AdmissionRequest
// ---------------------------------------------------------------------------

/// The typed admission request passed to a [`Validator`][crate::webhook::Validator].
///
/// `T` is the Kubernetes resource type being admitted. The `object` field
/// holds the incoming resource state (present on CREATE and UPDATE). For
/// UPDATE requests, `old_object` holds the prior state. For DELETE requests,
/// `old_object` typically holds the resource being deleted and `object` may
/// be absent.
///
/// # Examples
///
/// ```no_run
/// use koprs_admission::{AdmissionRequest, Operation};
///
/// fn check<T>(request: &AdmissionRequest<T>) {
///     if request.operation == Operation::Delete {
///         println!("deleting {} in {:?}", request.name, request.namespace);
///     }
/// }
/// ```
pub struct AdmissionRequest<T> {
    /// The unique identifier of this admission request, echoed back in the
    /// response so Kubernetes can correlate the two.
    pub uid: String,
    /// Name of the resource being admitted.
    pub name: String,
    /// Namespace of the resource, or `None` for cluster-scoped resources.
    pub namespace: Option<String>,
    /// The Kubernetes API operation that triggered this request.
    pub operation: Operation,
    /// The incoming (new) resource state. Present on CREATE and UPDATE;
    /// may be absent on DELETE depending on the Kubernetes version.
    pub object: Option<T>,
    /// The previous resource state. Present on UPDATE and DELETE.
    pub old_object: Option<T>,
    /// Whether the request is a dry-run (the resource will not be persisted).
    pub dry_run: bool,
}

// ---------------------------------------------------------------------------
// ValidationResponse
// ---------------------------------------------------------------------------

/// The decision returned by a [`Validator`][crate::webhook::Validator].
///
/// Use the constructor methods rather than constructing the struct directly:
///
/// | Method | When to use |
/// |--------|------------|
/// | [`allow`][Self::allow] | Accept the resource unconditionally |
/// | [`deny`][Self::deny] | Reject with a human-readable reason |
/// | [`allow_with_warnings`][Self::allow_with_warnings] | Accept but surface non-blocking messages |
///
/// # Examples
///
/// ```
/// use koprs_admission::ValidationResponse;
///
/// // Allow
/// let r = ValidationResponse::allow();
/// assert!(r.allowed);
///
/// // Deny with a reason
/// let r = ValidationResponse::deny("image tag 'latest' is not permitted");
/// assert!(!r.allowed);
/// assert_eq!(r.message.as_deref(), Some("image tag 'latest' is not permitted"));
/// ```
pub struct ValidationResponse {
    /// Whether the admission request is approved.
    pub allowed: bool,
    /// Human-readable message shown to the user when `allowed` is `false`.
    /// Also visible on `kubectl describe` output for denials.
    pub message: Option<String>,
    /// Non-blocking advisory messages attached to the response. Warnings
    /// are surfaced to the user on `kubectl apply` even when `allowed` is
    /// `true`.
    pub warnings: Vec<String>,
}

impl ValidationResponse {
    /// Allow the resource unconditionally.
    pub fn allow() -> Self {
        Self {
            allowed: true,
            message: None,
            warnings: vec![],
        }
    }

    /// Deny the resource with a human-readable reason.
    ///
    /// The message is shown to the user via `kubectl` when the admission
    /// request is rejected.
    pub fn deny(message: impl Into<String>) -> Self {
        Self {
            allowed: false,
            message: Some(message.into()),
            warnings: vec![],
        }
    }

    /// Allow the resource but attach advisory warnings.
    ///
    /// Warnings are surfaced to the user by `kubectl` even though the request
    /// is accepted.
    pub fn allow_with_warnings(warnings: Vec<String>) -> Self {
        Self {
            allowed: true,
            message: None,
            warnings,
        }
    }
}

// ---------------------------------------------------------------------------
// Internal parsing / serialisation helpers
// ---------------------------------------------------------------------------

/// Extract the request UID from a raw `AdmissionReview` JSON body.
///
/// Returns an empty string when the field is absent — the response will still
/// be structurally valid, and Kubernetes will log a correlation warning.
pub(crate) fn parse_uid(body: &Value) -> String {
    body.get("request")
        .and_then(|r| r.get("uid"))
        .and_then(|u| u.as_str())
        .unwrap_or("")
        .to_string()
}

/// Parse a raw `AdmissionReview` JSON body into a typed [`AdmissionRequest`].
pub(crate) fn parse_request<T: DeserializeOwned>(body: &Value) -> Result<AdmissionRequest<T>> {
    let request = body.get("request").ok_or_else(|| {
        AdmissionError::Internal("AdmissionReview missing 'request' field".into())
    })?;

    let uid = request
        .get("uid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let name = request
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let namespace = request
        .get("namespace")
        .and_then(|v| v.as_str())
        .map(String::from);

    let operation = request
        .get("operation")
        .and_then(|v| v.as_str())
        .map(Operation::from_str)
        .unwrap_or(Operation::Unknown("".into()));

    let object = request
        .get("object")
        .filter(|v| !v.is_null())
        .map(|v| serde_json::from_value::<T>(v.clone()))
        .transpose()
        .map_err(AdmissionError::Serialization)?;

    let old_object = request
        .get("oldObject")
        .filter(|v| !v.is_null())
        .map(|v| serde_json::from_value::<T>(v.clone()))
        .transpose()
        .map_err(AdmissionError::Serialization)?;

    let dry_run = request
        .get("dryRun")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    Ok(AdmissionRequest {
        uid,
        name,
        namespace,
        operation,
        object,
        old_object,
        dry_run,
    })
}

/// Build an `AdmissionReview` response JSON from a [`ValidationResponse`].
pub(crate) fn build_response(uid: &str, resp: &ValidationResponse) -> Value {
    let mut response = serde_json::json!({
        "uid": uid,
        "allowed": resp.allowed,
    });

    if let Some(msg) = &resp.message {
        response["status"] = serde_json::json!({
            "code": if resp.allowed { 200u16 } else { 403u16 },
            "message": msg,
        });
    }

    if !resp.warnings.is_empty() {
        response["warnings"] = serde_json::json!(resp.warnings);
    }

    serde_json::json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "response": response,
    })
}

/// Build a deny `AdmissionReview` response for internal errors (e.g. parse
/// failures). Uses HTTP status 400 to indicate a malformed request.
pub(crate) fn build_deny_response(uid: &str, message: &str) -> Value {
    serde_json::json!({
        "apiVersion": "admission.k8s.io/v1",
        "kind": "AdmissionReview",
        "response": {
            "uid": uid,
            "allowed": false,
            "status": {
                "code": 400u16,
                "message": message,
            },
        },
    })
}
