// src/main.rs
//
// Wires together the controller loop using koprs::controller::ControllerBuilder.
// Cross-resource trigger: changes to any ConfigMap carrying our managed-by
// label re-queue the owning ConfigMapSync CR.

mod reconciler;
mod types;

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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,koprs=debug".into()),
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
        .run(ConfigMapSyncReconciler, ctx)
        .await?;

    Ok(())
}
