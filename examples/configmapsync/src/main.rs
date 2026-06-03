// src/main.rs
//
// Wires together the controller loop using koprs::controller::ControllerBuilder.
//
// Secondary watches — each .watch() call composes onto the chain:
//   ConfigMap  — managed ConfigMaps re-queue the owning CR (active feature)
//   Secret     — demonstrates chaining; extend the reconciler to also manage
//                Secrets in the target namespace using the same owner label
//
// Operational features:
//   .health_port(8080)         — GET /healthz + GET /readyz for pod probes
//   .graceful_shutdown()       — clean stop on SIGTERM / Ctrl+C
//   .leader_election(...)      — Kubernetes Lease-based HA; only one replica reconciles
//   .reconcile_timeout(300s)   — kills and requeues reconciles stuck longer than 5 minutes

mod reconciler;
mod types;

use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};
use tracing::info;

use koprs::controller::{Context, ControllerBuilder, watcher};
use koprs::owners::owner_label_mapper;

use crate::reconciler::ConfigMapSyncReconciler;
use crate::types::ConfigMapSync;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("starting configmapsync-operator");

    let client = Client::try_default().await?;

    // Primary watched resource — all ConfigMapSync CRs across all namespaces.
    let cms_api: Api<ConfigMapSync> = Api::all(client.clone());

    // Secondary watched resources — both carry the same owner label convention.
    // .watch() calls compose: both watches are wired simultaneously.
    let cm_api: Api<ConfigMap> = Api::all(client.clone());

    let ctx = Context::new(client);

    // The operator namespace is injected via the downward API in production:
    //   env:
    //     - name: OPERATOR_NAMESPACE
    //       valueFrom:
    //         fieldRef:
    //           fieldPath: metadata.namespace
    let operator_ns = std::env::var("OPERATOR_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let labels = "app.kubernetes.io/managed-by=configmapsync-operator";

    ControllerBuilder::new(cms_api)
        // Watch: whenever a managed ConfigMap changes, re-queue the owning CR.
        .watch(
            cm_api,
            watcher::Config::default().labels(labels),
            owner_label_mapper("configmapsync.example.io/owner"),
        )
        .health_port(8080)
        .graceful_shutdown()
        .leader_election(operator_ns, "configmapsync-operator-leader")
        .reconcile_timeout(Duration::from_secs(300))
        .run(ConfigMapSyncReconciler, ctx)
        .await?;

    Ok(())
}
