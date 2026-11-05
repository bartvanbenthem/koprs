// src/main.rs
//
// Wires together a controller loop and a validating admission webhook, running
// both concurrently on the same Tokio runtime.
//
// Controller — koprs::controller::ControllerBuilder
//   Reconciles ApiConfigSync CRs: polls the configured external HTTP API
//   (koprs-external) and syncs the response body into a ConfigMap in the
//   target namespace.
//
//   .health_port(8080)         — GET /healthz + GET /readyz for pod probes
//   .metrics_port(9090)        — GET /metrics — Prometheus reconcile counts/errors/durations
//   .graceful_shutdown()       — clean stop on SIGTERM / Ctrl+C
//   .leader_election(...)      — Kubernetes Lease-based HA; only one replica reconciles.
//                                On lease loss `run` returns an error, `try_join!`
//                                fails, and the process exits non-zero so the pod
//                                restarts and campaigns again.
//   .reconcile_timeout(300s)   — kills and requeues reconciles stuck longer than 5 minutes
//
// Admission webhook — koprs_admission::WebhookBuilder
//   Validates ApiConfigSync resources before they are persisted. Denies
//   resources whose URL violates the outbound URL policy (url_policy.rs),
//   empty namespaces, or missing configKey. Attaches non-blocking warnings for
//   plain HTTP URLs or missing bearer token Secret references.
//
//   POST /validate/apiconfigsync — handled by ApiConfigSyncValidator
//   .port(8443)                — HTTPS (or plain HTTP in dev if no TLS certs found)
//   .health_port(8081)         — separate probe port so k8s can target the webhook pod
//   .graceful_shutdown()       — clean stop on SIGTERM / Ctrl+C
//
// TLS certificates for the webhook are loaded from the paths set by
// WEBHOOK_TLS_CERT / WEBHOOK_TLS_KEY (default: /tls/tls.crt, /tls/tls.key) and
// reloaded automatically when cert-manager rotates them. When the files are
// absent the webhook falls back to plain HTTP, which is useful for local
// development and integration testing.

mod reconciler;
mod types;
mod url_policy;
mod validator;

use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};
use tracing::info;

use koprs::controller::{Context, ControllerBuilder, watcher};
use koprs::owners::owner_labels_mapper;
use koprs_admission::WebhookBuilder;

use crate::reconciler::{ApiConfigSyncReconciler, AppState};
use crate::types::ApiConfigSync;
use crate::url_policy::UrlPolicy;
use crate::validator::ApiConfigSyncValidator;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("starting apiconfigsync-operator");

    // This binary links both rustls crypto backends (aws-lc-rs via kube,
    // ring via reqwest / koprs-admission), so rustls cannot pick one on its
    // own and `Client::try_default` would panic. Select ring explicitly.
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls CryptoProvider was already installed"))?;

    let client = Client::try_default().await?;

    // The operator namespace is injected via the downward API in production:
    //   env:
    //     - name: OPERATOR_NAMESPACE
    //       valueFrom:
    //         fieldRef:
    //           fieldPath: metadata.namespace
    let operator_ns = std::env::var("OPERATOR_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    // Outbound URL policy shared by the webhook and the reconciler.
    let policy = UrlPolicy::from_env();
    info!(
        allowed_hosts = ?policy.allowed_hosts,
        allow_http = policy.allow_http,
        allow_private_networks = policy.allow_private_networks,
        "outbound URL policy"
    );

    // Drive the controller loop and webhook server concurrently.
    // If either task returns an error, the other is cancelled and the process exits.
    tokio::try_join!(
        run_controller(client, operator_ns, policy.clone()),
        run_webhook(policy),
    )?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Controller
// ---------------------------------------------------------------------------

async fn run_controller(
    client: Client,
    operator_ns: String,
    policy: UrlPolicy,
) -> anyhow::Result<()> {
    // Primary watched resource — all ApiConfigSync CRs across all namespaces.
    let acs_api: Api<ApiConfigSync> = Api::all(client.clone());

    // Secondary watch — managed ConfigMaps re-queue the owning CR on change.
    let cm_api: Api<ConfigMap> = Api::all(client.clone());

    let ctx = Context::with_data(client, AppState::new(policy));

    let labels = "app.kubernetes.io/managed-by=apiconfigsync-operator";

    ControllerBuilder::new(acs_api)
        .watch(
            cm_api,
            watcher::Config::default().labels(labels),
            // The ConfigMap lives in `spec.targetNamespace`, so the owner's
            // namespace comes from a label rather than the ConfigMap's own.
            owner_labels_mapper(
                "apiconfigsync.example.io/owner",
                "apiconfigsync.example.io/owner-namespace",
            ),
        )
        .health_port(8080)
        .metrics_port(9090)
        .graceful_shutdown()
        .leader_election(operator_ns, "apiconfigsync-operator-leader")
        .reconcile_timeout(Duration::from_secs(300))
        .run(ApiConfigSyncReconciler, ctx)
        .await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Admission webhook
// ---------------------------------------------------------------------------

async fn run_webhook(policy: UrlPolicy) -> anyhow::Result<()> {
    let cert_path =
        std::env::var("WEBHOOK_TLS_CERT").unwrap_or_else(|_| "/tls/tls.crt".to_string());
    let key_path = std::env::var("WEBHOOK_TLS_KEY").unwrap_or_else(|_| "/tls/tls.key".to_string());

    let mut builder = WebhookBuilder::new()
        .port(8443)
        .health_port(8081)
        .graceful_shutdown()
        .validate("/validate/apiconfigsync", ApiConfigSyncValidator { policy });

    // Serve TLS when the cert-manager Secret volume is mounted; the files are
    // re-read when cert-manager rotates them, so no restart is needed.
    // Falls back to plain HTTP when the files are absent (local dev / CI).
    if std::path::Path::new(&cert_path).exists() && std::path::Path::new(&key_path).exists() {
        info!(cert = %cert_path, "admission webhook: TLS enabled (auto-reload)");
        builder = builder.tls_from_files(&cert_path, &key_path)?;
    } else {
        info!("admission webhook: TLS cert/key not found — serving plain HTTP (dev mode)");
    }

    builder.run().await?;
    Ok(())
}
