use futures::TryStreamExt;
use kube::{Api, Client, ResourceExt};
use kube_runtime::WatchStreamExt;
use kube_runtime::watcher::{Config, watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::error::Result;
use crate::scope::{ApiScope, Cluster, Namespaced};
use crate::traits::{ClusterResource, KubeResource, NamespacedResource};

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

async fn spawn_watcher<T>(api: Api<T>, config: Config, tx: mpsc::Sender<()>) -> Result<JoinHandle<()>>
where
    T: KubeResource,
{
    let handle = tokio::task::spawn(async move {
        let result = watcher(api, config)
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

// ---------------------------------------------------------------------------
// Generic public API
// ---------------------------------------------------------------------------

/// Watch resources of type `T` and send a signal on `tx` whenever a resource
/// is applied (created or updated).
///
/// An optional `label_selector` narrows the watch to matching resources only.
/// Pass `None` to watch all resources of the given type.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time. Prefer the convenience wrappers
/// ([`watch_namespaced`], [`watch_cluster`], [`watch_namespaced_by_label`],
/// [`watch_cluster_by_label`]) for the common cases — they are thin wrappers
/// around this function.
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch;
/// use kube_genops::scope::Namespaced;
/// use kube_genops::traits::NamespacedResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch::<MyCR, _>(
///     client,
///     Namespaced("my-namespace"),
///     None,
///     tx,
/// ).await?;
///
/// while let Some(()) = rx.recv().await {
///     println!("Resource changed!");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn watch<T, Scope>(
    client: Client,
    scope: Scope,
    label_selector: Option<&str>,
    tx: mpsc::Sender<()>,
) -> Result<JoinHandle<()>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match label_selector {
        Some(sel) => info!(%kind, label_selector = %sel, "Starting label-filtered resource watcher"),
        None      => info!(%kind, "Starting resource watcher"),
    }

    let config = match label_selector {
        Some(sel) => Config::default().labels(sel),
        None      => Config::default(),
    };

    spawn_watcher(scope.into_api(client), config, tx).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — namespaced
// ---------------------------------------------------------------------------

/// Watch all resources of type `T` in a namespace and send a signal on `tx`
/// whenever a resource is applied (created or updated).
///
/// Delegates to [`watch`] with [`Namespaced`] as the scope and no label
/// selector. The resource type `T` must implement [`NamespacedResource`],
/// which the compiler enforces — passing a cluster-scoped type is a compile
/// error.
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch_namespaced;
/// use kube_genops::traits::NamespacedResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_namespaced::<MyCR>(client, "my-namespace", tx).await?;
///
/// while let Some(()) = rx.recv().await {
///     println!("Resource changed!");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn watch_namespaced<T>(
    client: Client,
    namespace: &str,
    tx: mpsc::Sender<()>,
) -> Result<JoinHandle<()>>
where
    T: NamespacedResource,
{
    watch::<T, _>(client, Namespaced(namespace), None, tx).await
}

/// Watch resources of type `T` in a namespace matching a label selector, and
/// send a signal on `tx` whenever a matching resource is applied (created or
/// updated).
///
/// Delegates to [`watch`] with [`Namespaced`] as the scope. The resource type
/// `T` must implement [`NamespacedResource`].
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch_namespaced_by_label;
/// use kube_genops::traits::NamespacedResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_namespaced_by_label::<MyCR>(
///     client,
///     "my-namespace",
///     "app=my-operator",
///     tx,
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn watch_namespaced_by_label<T>(
    client: Client,
    namespace: &str,
    label_selector: &str,
    tx: mpsc::Sender<()>,
) -> Result<JoinHandle<()>>
where
    T: NamespacedResource,
{
    watch::<T, _>(client, Namespaced(namespace), Some(label_selector), tx).await
}

// ---------------------------------------------------------------------------
// Convenience wrappers — cluster-scoped
// ---------------------------------------------------------------------------

/// Watch all resources of type `T` cluster-wide and send a signal on `tx`
/// whenever a resource is applied (created or updated).
///
/// Delegates to [`watch`] with [`Cluster`] as the scope and no label selector.
/// The resource type `T` must implement [`ClusterResource`], which the
/// compiler enforces — passing a namespace-scoped type is a compile error.
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch_cluster;
/// use kube_genops::traits::ClusterResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_cluster::<MyCR>(client, tx).await?;
///
/// while let Some(()) = rx.recv().await {
///     println!("Resource changed!");
/// }
/// # Ok(())
/// # }
/// ```
pub async fn watch_cluster<T>(
    client: Client,
    tx: mpsc::Sender<()>,
) -> Result<JoinHandle<()>>
where
    T: ClusterResource,
{
    watch::<T, _>(client, Cluster, None, tx).await
}

/// Watch resources of type `T` cluster-wide matching a label selector, and
/// send a signal on `tx` whenever a matching resource is applied (created or
/// updated).
///
/// Delegates to [`watch`] with [`Cluster`] as the scope. The resource type
/// `T` must implement [`ClusterResource`].
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Examples
///
/// ```no_run
/// use kube::Client;
/// use kube_genops::watcher::watch_cluster_by_label;
/// use kube_genops::traits::ClusterResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: ClusterResource>(client: Client) -> anyhow::Result<()> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_cluster_by_label::<MyCR>(
///     client,
///     "app=my-operator",
///     tx,
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn watch_cluster_by_label<T>(
    client: Client,
    label_selector: &str,
    tx: mpsc::Sender<()>,
) -> Result<JoinHandle<()>>
where
    T: ClusterResource,
{
    watch::<T, _>(client, Cluster, Some(label_selector), tx).await
}