use std::fmt::Debug;

use futures::TryStreamExt;
use k8s_openapi::Metadata;
use kube::core::ObjectMeta;
use kube::{Api, Client, Resource, ResourceExt};
use kube_runtime::WatchStreamExt;
use kube_runtime::watcher::{watcher, Config};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::error::Result;

/// Watch all resources of type `T` across all namespaces and send a signal
/// on `tx` whenever a resource is applied (created or updated).
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Example
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch_resources;
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use tokio::sync::mpsc;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_resources::<ConfigMap>(client, tx).await?;
///
/// while let Some(()) = rx.recv().await {
///     println!("ConfigMap changed!");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn watch_resources<T>(client: Client, tx: mpsc::Sender<()>) -> Result<JoinHandle<()>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api = Api::<T>::all(client);
    let kind = T::kind(&());
    info!(%kind, "Starting resource watcher");

    let handle = tokio::task::spawn(async move {
        let result = watcher(api, Config::default())
            .applied_objects()
            .default_backoff()
            .try_for_each(|resource: T| {
                let tx = tx.clone();
                async move {
                    info!(name = %resource.name_any(), "Resource applied");
                    tx.send(()).await.ok();
                    Ok(())
                }
            })
            .await;

        if let Err(e) = result {
            error!(error = %e, "Resource watcher failed");
        }
    });

    Ok(handle)
}

/// Watch resources of type `T` matching a label selector and send a signal
/// on `tx` whenever a matching resource is applied (created or updated).
///
/// # Example
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch_resources_by_label;
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use tokio::sync::mpsc;
///
/// # async fn example(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_resources_by_label::<ConfigMap>(client, tx, "app=my-operator").await?;
/// # Ok(())
/// # }
/// ```
pub async fn watch_resources_by_label<T>(
    client: Client,
    tx: mpsc::Sender<()>,
    label_selector: &str,
) -> Result<JoinHandle<()>>
where
    T: Clone
        + Debug
        + Resource<DynamicType = ()>
        + Metadata<Ty = ObjectMeta>
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static,
{
    let api = Api::<T>::all(client);
    let config = Config::default().labels(label_selector);
    let kind = T::kind(&());
    info!(%kind, %label_selector, "Starting label-filtered resource watcher");

    let handle = tokio::task::spawn(async move {
        let result = watcher(api, config)
            .applied_objects()
            .default_backoff()
            .try_for_each(|resource: T| {
                let tx = tx.clone();
                async move {
                    info!(name = %resource.name_any(), "Labeled resource applied");
                    tx.send(()).await.ok();
                    Ok(())
                }
            })
            .await;

        if let Err(e) = result {
            error!(error = %e, "Labeled resource watcher failed");
        }
    });

    Ok(handle)
}