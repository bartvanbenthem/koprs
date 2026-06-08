// src/types.rs
//
// Defines two independent CRDs that this operator manages concurrently:
//
//   SecretSync         — ensures a Secret with the given string data exists
//                        in a target namespace.
//   ServiceAccountSync — ensures a ServiceAccount with the given image-pull
//                        secrets exists in a target namespace.
//
// Each CRD is reconciled by its own controller (see secretsync.rs and
// serviceaccountsync.rs); main.rs runs both controllers side by side.

use std::collections::BTreeMap;

use koprs::status::KoprsCondition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec section of the SecretSync CRD.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[kube(
    group = "example.io",
    version = "v1alpha1",
    kind = "SecretSync",
    namespaced,
    status = "SecretSyncStatus",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetNamespace"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#
)]
pub struct SecretSyncSpec {
    /// Namespace where the Secret should be created/maintained.
    pub target_namespace: String,
    /// Plaintext key/value pairs to populate in the Secret's `stringData`.
    pub string_data: BTreeMap<String, String>,
}

/// Status section written back by the operator after each reconcile.
/// Must derive JsonSchema because SecretSync's CustomResource derive requires it.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SecretSyncStatus {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub message: String,
    /// Standard Kubernetes conditions array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<KoprsCondition>,
}

/// Spec section of the ServiceAccountSync CRD.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[kube(
    group = "example.io",
    version = "v1alpha1",
    kind = "ServiceAccountSync",
    namespaced,
    status = "ServiceAccountSyncStatus",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetNamespace"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#
)]
pub struct ServiceAccountSyncSpec {
    /// Namespace where the ServiceAccount should be created/maintained.
    pub target_namespace: String,
    /// Whether pods using this ServiceAccount automount its token.
    #[serde(default)]
    pub automount_token: bool,
    /// Names of image-pull secrets to attach to the ServiceAccount.
    #[serde(default)]
    pub image_pull_secrets: Vec<String>,
}

/// Status section written back by the operator after each reconcile.
/// Must derive JsonSchema because ServiceAccountSync's CustomResource derive requires it.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountSyncStatus {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub message: String,
    /// Standard Kubernetes conditions array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<KoprsCondition>,
}
