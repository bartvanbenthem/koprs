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

/// Status section written back by the operator after each reconcile.
/// Must derive JsonSchema because ConfigMapSync's CustomResource derive requires it.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConfigMapSyncStatus {
    pub ready: bool,
    pub message: String,
}
