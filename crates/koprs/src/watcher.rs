use futures::TryStreamExt;
use kube::{Api, Client, ResourceExt};
use kube_runtime::WatchStreamExt;
use kube_runtime::watcher::{Config, watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::error::Result;
use crate::scope::ApiScope;
use crate::traits::KubeResource;

// ---------------------------------------------------------------------------
// Private core helpers
// ---------------------------------------------------------------------------

async fn spawn_watcher<T>(
    api: Api<T>,
    config: Config,
    tx: mpsc::Sender<()>,
) -> Result<JoinHandle<()>>
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
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::watcher::watch;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
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
        Some(sel) => {
            info!(%kind, label_selector = %sel, "Starting label-filtered resource watcher")
        }
        None => info!(%kind, "Starting resource watcher"),
    }

    let config = match label_selector {
        Some(sel) => Config::default().labels(sel),
        None => Config::default(),
    };

    spawn_watcher(scope.into_api(client), config, tx).await
}
