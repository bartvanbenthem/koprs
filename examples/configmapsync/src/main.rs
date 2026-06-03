// src/main.rs
//
// Wires together the controller loop using koprs::controller::ControllerBuilder.
// Cross-resource trigger: changes to any ConfigMap carrying our managed-by
// label re-queue the owning ConfigMapSync CR.
//
// Operational features wired in:
//   .health_port(8080)         — GET /healthz + GET /readyz for pod probes
//   .graceful_shutdown()       — clean stop on SIGTERM / Ctrl+C
//   .leader_election(...)      — Kubernetes Lease-based HA; only one replica reconciles
//   .reconcile_timeout(300s)   — kills and requeues reconciles stuck longer than 5 minutes

mod reconciler;
mod types;

use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client, ResourceExt};
use tracing::info;

use koprs::controller::{Context, ControllerBuilder, ObjectRef, watcher};

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

    // Secondary watched resource — ConfigMaps carrying our managed-by label.
    // Whenever one changes we re-queue the owning ConfigMapSync CR.
    let cm_api: Api<ConfigMap> = Api::all(client.clone());

    let ctx = Context::new(client);

    // The operator namespace is injected via the downward API in production:
    //   env:
    //     - name: OPERATOR_NAMESPACE
    //       valueFrom:
    //         fieldRef:
    //           fieldPath: metadata.namespace
    let operator_ns = std::env::var("OPERATOR_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    ControllerBuilder::new(cms_api)
        .with_watches(move |ctl| {
            ctl.watches(
                cm_api,
                watcher::Config::default()
                    .labels("app.kubernetes.io/managed-by=configmapsync-operator"),
                |cm| {
                    // The owner label on the ConfigMap tells us which CR to re-queue.
                    let owner = cm
                        .labels()
                        .get("configmapsync.example.io/owner")
                        .cloned()
                        .unwrap_or_default();
                    let ns = cm.namespace().unwrap_or_default();
                    if owner.is_empty() || ns.is_empty() {
                        return None;
                    }
                    Some(ObjectRef::<ConfigMapSync>::new(&owner).within(&ns))
                },
            )
        })
        .health_port(8080)
        .graceful_shutdown()
        .leader_election(operator_ns, "configmapsync-operator-leader")
        .reconcile_timeout(Duration::from_secs(300))
        .run(ConfigMapSyncReconciler, ctx)
        .await?;

    Ok(())
}
