// src/reconciler.rs

use std::collections::BTreeMap;
use std::sync::Arc;

use k8s_openapi::api::core::v1::{ConfigMap, Secret};
use kube::{Api, ResourceExt};
use tokio::time::Duration;
use tracing::{error, info, warn};

use koprs::controller::{Action, Context, Reconciler};
use koprs::error::KubeGenericError;
use koprs::events::{EventType, record_event};
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizer};
use koprs::gc::gc_resources;
use koprs::is_being_deleted;
use koprs::meta::ObjectMetaBuilder;
use koprs::resources::{EnsureOutcome, delete_resource, ensure_resource, patch_labels};
use koprs::scope::Namespaced;
use koprs::status::{make_condition, patch_status_namespaced, upsert_condition};

use koprs_external::http::HttpPoller;
use koprs_external::watcher::{ExternalEvent, ExternalSource};

use crate::types::{ApiConfigSync, ApiConfigSyncStatus, SecretKeyRef};
use crate::url_policy::{MAX_BODY_BYTES, UrlPolicy};

const FINALIZER: &str = "apiconfigsync.example.io/cleanup";
const FIELD_MANAGER: &str = "apiconfigsync-operator";
const MANAGED_LABEL: &str = "app.kubernetes.io/managed-by=apiconfigsync-operator";

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Shared across all reconciles via `Context::with_data`.
pub struct AppState {
    /// Outbound URL policy, re-checked on every reconcile.
    pub policy: UrlPolicy,
    /// One client for all reconciles: connection pooling, timeouts, no
    /// redirects, and a DNS resolver that refuses non-public addresses.
    pub http: reqwest::Client,
}

impl AppState {
    pub fn new(policy: UrlPolicy) -> Self {
        let http = policy.http_client();
        Self { policy, http }
    }
}

// ---------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------

pub struct ApiConfigSyncReconciler;

impl Reconciler<ApiConfigSync, AppState> for ApiConfigSyncReconciler {
    type Error = KubeGenericError;

    async fn reconcile(
        &self,
        cr: Arc<ApiConfigSync>,
        ctx: Arc<Context<AppState>>,
    ) -> Result<Action, KubeGenericError> {
        let client = ctx.client.clone();
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(KubeGenericError::MissingMetadata("namespace".into()))?;

        info!(cr = %name, ns = %namespace, "reconciling ApiConfigSync");

        // -------------------------------------------------------------------
        // Deletion path
        // -------------------------------------------------------------------
        if is_being_deleted(&*cr) {
            info!(cr = %name, "deletion timestamp set — running cleanup");

            let target_ns = &cr.spec.target_namespace;
            let cm_name = configmap_name(&namespace, &name);

            match delete_resource::<ConfigMap, _>(client.clone(), Namespaced(target_ns), &cm_name)
                .await
            {
                Ok(true) => info!(cm = %cm_name, ns = %target_ns, "deleted synced ConfigMap"),
                Ok(false) => info!(cm = %cm_name, "ConfigMap was already gone"),
                Err(e) => {
                    error!(error = %e, "failed to delete ConfigMap during cleanup");
                    return Err(e);
                }
            }

            // Remove only our finalizer; others (if any) stay in place.
            remove_finalizer::<ApiConfigSync, _>(
                client.clone(),
                Namespaced(&namespace),
                &*cr,
                FINALIZER,
            )
            .await?;
            info!(cr = %name, "finalizer removed — deletion complete");
            return Ok(Action::await_change());
        }

        // -------------------------------------------------------------------
        // Normal reconcile path
        // -------------------------------------------------------------------

        // 1. Ensure finalizer is present.
        add_finalizer_namespaced::<ApiConfigSync>(client.clone(), &cr, FINALIZER).await?;

        // 2. Enforce the outbound URL policy. The admission webhook checks it
        //    too, but the webhook can be bypassed or misconfigured.
        if let Err(reason) = ctx.data.policy.check(&cr.spec.api_url) {
            warn!(cr = %name, %reason, "refusing to poll spec.apiUrl");
            return Err(KubeGenericError::Config(reason));
        }

        // 3. Poll the external API once to fetch the current config.
        //
        //    A fresh HttpPoller (sharing the pooled client) is created each
        //    reconcile. Because `seen` starts false it always emits Added on
        //    this single call, which is intended: we always want the latest
        //    data. The body is capped at the ConfigMap size limit.
        let target_ns = &cr.spec.target_namespace;
        let cm_name = configmap_name(&namespace, &name);

        let token = match &cr.spec.bearer_token_secret_ref {
            Some(r) => Some(read_token(client.clone(), &namespace, r).await?),
            None => None,
        };
        let mut poller = build_poller(
            ctx.data.http.clone(),
            &cr.spec.api_url,
            token.as_deref(),
            &namespace,
            &name,
        );

        let config_body = match poller.poll().await {
            Ok(events) => match events.into_iter().next() {
                Some(ExternalEvent::Added(r) | ExternalEvent::Modified(r)) => {
                    String::from_utf8_lossy(&r.body).into_owned()
                }
                Some(ExternalEvent::Removed(_)) | None => {
                    warn!(
                        cr = %name,
                        url = %cr.spec.api_url,
                        "API endpoint returned no content — ConfigMap will be empty"
                    );
                    String::new()
                }
            },
            Err(e) => {
                error!(cr = %name, error = %e, "failed to poll external API");
                return Err(KubeGenericError::Internal(e.to_string()));
            }
        };

        // 4. Build and apply the ConfigMap from the fetched body.
        let desired_cm = build_configmap(
            &cm_name,
            target_ns,
            &namespace,
            &name,
            &cr.spec.config_key,
            &config_body,
        );

        let outcome = ensure_resource::<ConfigMap, _>(
            client.clone(),
            Namespaced(target_ns),
            &desired_cm,
            FIELD_MANAGER,
        )
        .await?;
        info!(cm = %cm_name, ns = %target_ns, "applied ConfigMap");

        if outcome.was_changed() {
            let (reason, note) = match &outcome {
                EnsureOutcome::Created(_) => (
                    "ConfigMapCreated",
                    format!("ConfigMap '{cm_name}' created in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Updated(_) => (
                    "ConfigMapUpdated",
                    format!("ConfigMap '{cm_name}' updated in namespace '{target_ns}'"),
                ),
                EnsureOutcome::Unchanged(_) => unreachable!(),
            };
            record_event(
                client.clone(),
                &*cr,
                EventType::Normal,
                "Sync",
                reason,
                note,
                FIELD_MANAGER,
            )
            .await?;
        }

        // 5. Garbage-collect stale ConfigMaps previously owned by this CR.
        gc_resources::<ConfigMap, _>(
            client.clone(),
            Namespaced(target_ns),
            &owner_selector(&namespace, &name),
            |cm| cm.name_any() == cm_name,
        )
        .await?;

        // 6. Stamp the target namespace as a label on the CR.
        patch_labels::<ApiConfigSync, _>(
            client.clone(),
            Namespaced(&namespace),
            &name,
            &[("apiconfigsync.example.io/synced-to", target_ns)],
        )
        .await?;

        // 7. Write the full status in one SSA patch.
        let generation = cr.metadata.generation;
        let status_message = format!(
            "ConfigMap '{cm_name}' synced from '{}' to namespace '{target_ns}'",
            cr.spec.api_url
        );

        let mut conditions = cr
            .status
            .as_ref()
            .map(|s| s.conditions.clone())
            .unwrap_or_default();
        upsert_condition(
            &mut conditions,
            make_condition(
                "Ready",
                "True",
                "ApiConfigSynced",
                &status_message,
                generation,
            ),
        );

        patch_status_namespaced::<ApiConfigSync, ApiConfigSyncStatus>(
            client.clone(),
            &namespace,
            &name,
            ApiConfigSyncStatus {
                ready: true,
                message: status_message,
                conditions,
            },
            FIELD_MANAGER,
        )
        .await?;

        info!(cr = %name, "reconcile complete");
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    fn error_label(&self, err: &KubeGenericError) -> &'static str {
        err.metric_label()
    }

    fn error_policy(
        &self,
        cr: Arc<ApiConfigSync>,
        error: &KubeGenericError,
        _ctx: Arc<Context<AppState>>,
    ) -> Action {
        error!(cr = %cr.name_any(), error = %error, "reconcile failed — retrying in 5s");
        Action::requeue(Duration::from_secs(5))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Child name; includes the CR namespace so equally-named CRs in different
/// namespaces never write the same ConfigMap.
fn configmap_name(cr_namespace: &str, cr_name: &str) -> String {
    format!("acs-{cr_namespace}-{cr_name}")
}

/// Label selector matching only the ConfigMaps of one CR. GC with a selector
/// shared by all CRs would make every CR delete the others' ConfigMaps.
fn owner_selector(cr_namespace: &str, cr_name: &str) -> String {
    format!(
        "{MANAGED_LABEL},apiconfigsync.example.io/owner={cr_name},\
         apiconfigsync.example.io/owner-namespace={cr_namespace}"
    )
}

/// Read the bearer token from a Secret in the CR's own namespace.
async fn read_token(
    client: kube::Client,
    namespace: &str,
    secret_ref: &SecretKeyRef,
) -> Result<String, KubeGenericError> {
    let secret = Api::<Secret>::namespaced(client, namespace)
        .get_opt(&secret_ref.name)
        .await?
        .ok_or_else(|| {
            KubeGenericError::Config(format!(
                "bearer token Secret '{namespace}/{}' not found",
                secret_ref.name
            ))
        })?;
    let bytes = secret
        .data
        .as_ref()
        .and_then(|d| d.get(&secret_ref.key))
        .ok_or_else(|| {
            KubeGenericError::Config(format!(
                "Secret '{namespace}/{}' has no key '{}'",
                secret_ref.name, secret_ref.key
            ))
        })?;
    let token = String::from_utf8(bytes.0.clone()).map_err(|_| {
        KubeGenericError::Config(format!(
            "Secret '{namespace}/{}' key '{}' is not valid UTF-8",
            secret_ref.name, secret_ref.key
        ))
    })?;
    Ok(token.trim().to_string())
}

fn build_poller(
    http: reqwest::Client,
    url: &str,
    bearer_token: Option<&str>,
    cr_namespace: &str,
    cr_name: &str,
) -> HttpPoller {
    let poller = HttpPoller::new(url)
        .with_client(http)
        .with_max_body_size(MAX_BODY_BYTES)
        .with_name(format!("apiconfigsync/{cr_namespace}/{cr_name}"));
    match bearer_token {
        Some(token) => poller.with_bearer_token(token),
        None => poller,
    }
}

fn build_configmap(
    name: &str,
    namespace: &str,
    owner_ns: &str,
    owner_cr: &str,
    config_key: &str,
    config_body: &str,
) -> ConfigMap {
    let mut data = BTreeMap::new();
    data.insert(config_key.to_string(), config_body.to_string());

    ConfigMap {
        metadata: ObjectMetaBuilder::new()
            .name(name)
            .namespace(namespace)
            .label("app.kubernetes.io/managed-by", "apiconfigsync-operator")
            .label("apiconfigsync.example.io/owner", owner_cr)
            .label("apiconfigsync.example.io/owner-namespace", owner_ns)
            .build(),
        data: Some(data),
        ..Default::default()
    }
}
