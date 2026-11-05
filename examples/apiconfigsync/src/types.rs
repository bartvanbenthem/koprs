// src/types.rs
//
// Defines the `ApiConfigSync` CRD.  The operator watches for instances of
// this resource and ensures a ConfigMap whose content is fetched from an
// external HTTP API exists in the specified target namespace.

use koprs::status::KoprsCondition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Spec section of the ApiConfigSync CRD.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
#[kube(
    group = "example.io",
    version = "v1alpha1",
    kind = "ApiConfigSync",
    namespaced,
    status = "ApiConfigSyncStatus",
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.targetNamespace"}"#,
    printcolumn = r#"{"name":"ApiUrl","type":"string","jsonPath":".spec.apiUrl"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.ready"}"#
)]
pub struct ApiConfigSyncSpec {
    /// Namespace where the synced ConfigMap should be created/maintained.
    pub target_namespace: String,
    /// HTTPS endpoint to poll once per reconcile for configuration data.
    /// Must satisfy the operator's outbound URL policy (see `url_policy.rs`).
    pub api_url: String,
    /// Optional reference to a Secret **in the CR's namespace** holding the
    /// Bearer token sent as `Authorization: Bearer <token>`.
    ///
    /// Tokens are never stored in the CR itself: CR specs are readable by
    /// anyone with `get` on the resource and end up in etcd, audit logs, and
    /// `kubectl get -o yaml` output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_token_secret_ref: Option<SecretKeyRef>,
    /// Key name used in the resulting ConfigMap (default: "config").
    #[serde(default = "default_config_key")]
    pub config_key: String,
}

/// Selects one key of a Secret in the same namespace as the CR.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Name of the Secret.
    pub name: String,
    /// Key within the Secret's `data` (default: "token").
    #[serde(default = "default_token_key")]
    pub key: String,
}

fn default_token_key() -> String {
    "token".to_string()
}

fn default_config_key() -> String {
    "config".to_string()
}

/// Status section written back by the operator after each reconcile.
#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiConfigSyncStatus {
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub message: String,
    /// Standard Kubernetes conditions array.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<KoprsCondition>,
}

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt;

    /// `manifests/crd.yaml` must match the CRD generated from these types.
    /// Regenerate it after changing the spec or status.
    #[test]
    fn crd_manifest_is_up_to_date() {
        let manifest = include_str!("../manifests/crd.yaml");
        let on_disk: serde_json::Value = serde_saphyr::from_str(manifest).unwrap();
        let generated = serde_json::to_value(super::ApiConfigSync::crd()).unwrap();
        assert_eq!(
            on_disk, generated,
            "manifests/crd.yaml is out of date with src/types.rs"
        );
    }
}
