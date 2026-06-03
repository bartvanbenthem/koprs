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
// WatchEvent
// ---------------------------------------------------------------------------

/// A Kubernetes resource event, distinguishing between a resource being
/// applied (created or modified) and being deleted.
///
/// Produced by [`watch_events`]. Use [`watch`] when only a trigger signal is
/// needed, or [`watch_objects`] when the resource data is needed on applies
/// but deletions can be ignored.
#[derive(Debug, Clone)]
pub enum WatchEvent<T> {
    /// The resource was created or modified.
    Applied(T),
    /// The resource was deleted.
    ///
    /// Note: events may be missed if the watcher was unavailable during the
    /// deletion. Use finalizers for reliable cleanup guarantees.
    Deleted(T),
}

// ---------------------------------------------------------------------------
// Private core helper
// ---------------------------------------------------------------------------

/// Shared task body for signal-only (`()`) and resource-data (`T`) watchers.
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
// watch — signal only
// ---------------------------------------------------------------------------

/// Watch resources of type `T` and send a unit signal on `tx` whenever a
/// resource is applied (created or updated).
///
/// An optional `label_selector` narrows the watch to matching resources only.
/// Pass `None` to watch all resources of the given type.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument to select the
/// correct API surface at compile time.
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// Use [`watch_objects`] when you need the resource data on each event, or
/// [`watch_events`] when you also need to react to deletions.
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
/// let _handle = watch::<MyCR, _>(client, Namespaced("my-namespace"), None, tx).await?;
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
        Some(sel) => info!(%kind, label_selector = %sel, "Starting signal watcher"),
        None => info!(%kind, "Starting signal watcher"),
    }

    let config = match label_selector {
        Some(sel) => Config::default().labels(sel),
        None => Config::default(),
    };

    spawn_watcher(scope.into_api(client), config, tx).await
}

// ---------------------------------------------------------------------------
// watch_objects — resource data on applies
// ---------------------------------------------------------------------------

/// Watch resources of type `T` and send the resource itself on `tx` whenever
/// it is applied (created or updated).
///
/// Like [`watch`] but sends `T` instead of `()`, so callers receive the
/// current resource state without a follow-up GET. Deletions are not reported;
/// use [`watch_events`] if deletion handling is required.
///
/// An optional `label_selector` narrows the watch to matching resources only.
/// Pass `None` to watch all resources of the given type.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::watcher::watch_objects;
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: NamespacedResource + std::fmt::Debug>(client: Client) -> Result<(), KubeGenericError> {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_objects::<MyCR, _>(client, Namespaced("my-namespace"), None, tx).await?;
///
/// while let Some(resource) = rx.recv().await {
///     println!("resource changed: {:?}", resource);
/// }
/// # Ok(())
/// # }
/// ```
pub async fn watch_objects<T, Scope>(
    client: Client,
    scope: Scope,
    label_selector: Option<&str>,
    tx: mpsc::Sender<T>,
) -> Result<JoinHandle<()>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match label_selector {
        Some(sel) => info!(%kind, label_selector = %sel, "Starting object watcher"),
        None => info!(%kind, "Starting object watcher"),
    }

    let config = match label_selector {
        Some(sel) => Config::default().labels(sel),
        None => Config::default(),
    };

    let api = scope.into_api(client);
    let handle = tokio::task::spawn(async move {
        let result = watcher(api, config)
            .applied_objects()
            .default_backoff()
            .try_for_each(|resource: T| {
                let tx = tx.clone();
                async move {
                    info!(name = %resource.name_any(), "Resource applied — sending object");
                    tx.send(resource).await.ok();
                    Ok(())
                }
            })
            .await;
        if let Err(e) = result {
            error!(error = %e, "Object watcher failed");
        }
    });
    Ok(handle)
}

// ---------------------------------------------------------------------------
// watch_events — full event model (applied + deleted)
// ---------------------------------------------------------------------------

/// Watch resources of type `T` and send a [`WatchEvent`] on `tx` for every
/// Kubernetes event, including deletions.
///
/// Each event is one of:
/// - [`WatchEvent::Applied`] — the resource was created, modified, or
///   re-observed during a watch restart.
/// - [`WatchEvent::Deleted`] — the resource was deleted.
///
/// An optional `label_selector` narrows the watch to matching resources only.
/// Pass `None` to watch all resources of the given type.
///
/// Pass [`Cluster`] or [`Namespaced`] as the `scope` argument.
///
/// Returns a [`JoinHandle`] for the background task. The task shuts down
/// automatically when all receivers are dropped.
///
/// > **Note:** Delete events may be lost if the watcher was unavailable during
/// > the deletion window. For reliable deletion guarantees, use finalizers.
///
/// # Examples
///
/// ```no_run
/// use koprs::error::KubeGenericError;
/// use kube::Client;
/// use koprs::watcher::{watch_events, WatchEvent};
/// use koprs::scope::Namespaced;
/// use koprs::traits::NamespacedResource;
/// use tokio::sync::mpsc;
///
/// # async fn example<MyCR: NamespacedResource>(client: Client) -> Result<(), KubeGenericError> {
/// use kube::ResourceExt;
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_events::<MyCR, _>(client, Namespaced("my-namespace"), None, tx).await?;
///
/// while let Some(event) = rx.recv().await {
///     match event {
///         WatchEvent::Applied(r) => println!("applied: {}", r.name_any()),
///         WatchEvent::Deleted(r) => println!("deleted: {}", r.name_any()),
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub async fn watch_events<T, Scope>(
    client: Client,
    scope: Scope,
    label_selector: Option<&str>,
    tx: mpsc::Sender<WatchEvent<T>>,
) -> Result<JoinHandle<()>>
where
    T: KubeResource,
    Scope: ApiScope<T>,
{
    let kind = T::kind(&());
    match label_selector {
        Some(sel) => info!(%kind, label_selector = %sel, "Starting event watcher"),
        None => info!(%kind, "Starting event watcher"),
    }

    let config = match label_selector {
        Some(sel) => Config::default().labels(sel),
        None => Config::default(),
    };

    let api = scope.into_api(client);
    let handle = tokio::task::spawn(async move {
        let result = watcher(api, config)
            .default_backoff()
            .try_for_each(|event| {
                let tx = tx.clone();
                async move {
                    let msg = match event {
                        kube_runtime::watcher::Event::Apply(r) => {
                            info!(name = %r.name_any(), "Resource applied");
                            Some(WatchEvent::Applied(r))
                        }
                        kube_runtime::watcher::Event::Delete(r) => {
                            info!(name = %r.name_any(), "Resource deleted");
                            Some(WatchEvent::Deleted(r))
                        }
                        // During a watch restart, existing objects arrive as
                        // InitApply events. Treat them as Applied so callers
                        // can reconcile current state.
                        kube_runtime::watcher::Event::InitApply(r) => {
                            info!(name = %r.name_any(), "Resource observed (init)");
                            Some(WatchEvent::Applied(r))
                        }
                        // Init and InitDone are bookmarks with no resource
                        // data — safe to skip.
                        kube_runtime::watcher::Event::Init
                        | kube_runtime::watcher::Event::InitDone => None,
                    };
                    if let Some(msg) = msg {
                        tx.send(msg).await.ok();
                    }
                    Ok(())
                }
            })
            .await;
        if let Err(e) = result {
            error!(error = %e, "Event watcher failed");
        }
    });
    Ok(handle)
}
