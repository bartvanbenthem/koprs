// src/main.rs
//
// Wires together the kube-runtime Controller with our reconciler.
// Cross-resource trigger: changes to any ConfigMap that carries our managed-by
// label will also re-queue the owning ConfigMapSync CR.

mod reconciler;
mod types;

use std::sync::Arc;

use futures_util::StreamExt;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::controller::Controller;
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Api, Client, ResourceExt};
use tracing::info;

use crate::reconciler::{Context, error_policy, reconcile};
use crate::types::ConfigMapSync;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialise tracing — respects RUST_LOG, defaults to INFO.
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

    let ctx = Arc::new(Context {
        client: client.clone(),
    });

    Controller::new(cms_api, watcher::Config::default())
        .watches(
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
        .run(reconcile, error_policy, ctx)
        .for_each(|result| async move {
            match result {
                Ok((obj, _action)) => {
                    info!(cr = %obj.name, "reconcile succeeded");
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("not found in local store") {
                        tracing::warn!(error = %e, "reconcile skipped — object not in local store");
                    } else {
                        tracing::error!(error = %e, "reconcile error");
                    }
                }
            }
        })
        .await;

    Ok(())
}
