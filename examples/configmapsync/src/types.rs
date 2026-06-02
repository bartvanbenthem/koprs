// src/types.rs
//
// Defines the `ConfigMapSync` CRD.  The operator watches for instances of
// this resource and ensures a ConfigMap with the given key/value pairs
// exists in the specified target namespace.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec section of the ConfigMapSync CRD.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[kube(
    group = "example.io",
    version = "v1alpha1",
    kind = "ConfigMapSync",
    namespaced,
    status = "ConfigMapSyncStatus",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetNamespace"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#
)]
pub struct ConfigMapSyncSpec {
    /// Namespace where the ConfigMap should be created/maintained.
    pub target_namespace: String,
    /// Key/value pairs to populate in the ConfigMap.
    pub data: std::collections::BTreeMap<String, String>,
}

/// A single status condition on a `ConfigMapSync` resource.
///
/// Mirrors the standard Kubernetes `metav1.Condition` shape so tooling such
/// as `kubectl` can render it correctly, but is defined here so it appears in
/// the CRD's OpenAPI schema (k8s_openapi's Condition does not derive
/// JsonSchema).
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
pub struct SyncCondition {
    /// The condition type, e.g. `"Ready"`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `"True"`, `"False"`, or `"Unknown"`.
    pub status: String,
    /// Machine-readable reason token, e.g. `"ConfigMapSynced"`.
    pub reason: String,
    /// Human-readable description of the condition.
    pub message: String,
    /// RFC 3339 timestamp of the last status transition.
    #[serde(rename = "lastTransitionTime")]
    pub last_transition_time: String,
    /// Generation of the CR observed when this condition was set.
    #[serde(rename = "observedGeneration", skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
}

/// Status section written back by the operator after each reconcile.
/// Must derive JsonSchema because ConfigMapSync's CustomResource derive requires it.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapSyncStatus {
    pub ready: bool,
    pub message: String,
    /// Standard Kubernetes conditions array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<SyncCondition>,
}
