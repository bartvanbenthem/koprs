use std::pin::pin;

use futures::{Stream, StreamExt};
use kube::{Client, ResourceExt};
use kube_runtime::WatchStreamExt;
use kube_runtime::watcher::{Config, Event, watcher};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::error::Result;
use crate::scope::ListScope;
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
// Private core helpers
// ---------------------------------------------------------------------------

/// Watcher configuration for an optional label selector.
fn watcher_config(label_selector: Option<&str>) -> Config {
    match label_selector {
        Some(selector) => Config::default().labels(selector),
        None => Config::default(),
    }
}

fn log_start<T: KubeResource>(watcher_kind: &'static str, label_selector: Option<&str>) {
    let kind = T::kind(&());
    match label_selector {
        Some(selector) => {
            info!(%kind, label_selector = %selector, watcher = watcher_kind, "Starting watcher")
        }
        None => info!(%kind, watcher = watcher_kind, "Starting watcher"),
    }
}

/// Forward the items of a watch stream to `tx` until the receiver is dropped.
///
/// `to_message` turns a stream item into the message to send, or `None` to
/// skip it.
///
/// A watch error does **not** end the task. kube's watcher recovers on the
/// next poll and `default_backoff` spaces the retries, so a transient API
/// failure (dropped connection, expired resource version, API server restart)
/// must not silently stop the caller's event feed. The task ends only when
/// every receiver is gone, which is checked while waiting for events too, so
/// an idle watch does not outlive its consumer.
async fn forward_watch<Item, Message, Watch>(
    watch_stream: Watch,
    tx: mpsc::Sender<Message>,
    to_message: fn(Item) -> Option<Message>,
) where
    Watch: Stream<Item = std::result::Result<Item, kube_runtime::watcher::Error>>,
{
    let mut watch_stream = pin!(watch_stream);
    loop {
        let next = tokio::select! {
            () = tx.closed() => break,
            next = watch_stream.next() => next,
        };
        match next {
            Some(Ok(item)) => {
                let Some(message) = to_message(item) else {
                    continue;
                };
                if tx.send(message).await.is_err() {
                    break;
                }
            }
            Some(Err(e)) => warn!(error = %e, "Watch failed, retrying with backoff"),
            None => {
                debug!("Watch stream ended");
                return;
            }
        }
    }
    debug!("Receiver dropped, stopping watcher");
}

fn applied_signal<T: KubeResource>(resource: T) -> Option<()> {
    debug!(name = %resource.name_any(), "Resource applied");
    Some(())
}

fn applied_object<T: KubeResource>(resource: T) -> Option<T> {
    debug!(name = %resource.name_any(), "Resource applied, sending object");
    Some(resource)
}

/// Map a raw watcher event to a [`WatchEvent`], skipping bookmarks.
fn to_watch_event<T: KubeResource>(event: Event<T>) -> Option<WatchEvent<T>> {
    match event {
        Event::Apply(resource) => {
            debug!(name = %resource.name_any(), "Resource applied");
            Some(WatchEvent::Applied(resource))
        }
        Event::Delete(resource) => {
            debug!(name = %resource.name_any(), "Resource deleted");
            Some(WatchEvent::Deleted(resource))
        }
        // During a watch restart, existing objects arrive as InitApply
        // events. Treat them as Applied so callers can reconcile current
        // state.
        Event::InitApply(resource) => {
            debug!(name = %resource.name_any(), "Resource observed (init)");
            Some(WatchEvent::Applied(resource))
        }
        // Init and InitDone are bookmarks with no resource data.
        Event::Init | Event::InitDone => None,
    }
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
/// Pass [`Cluster`][crate::scope::Cluster] or [`Namespaced`][crate::scope::Namespaced] as the `scope` argument to select the
/// correct API surface at compile time.
///
/// Returns a [`JoinHandle`] for the background task. Watch errors are logged
/// and retried with backoff; the task shuts down only when all receivers are
/// dropped (or the handle is aborted).
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
    Scope: ListScope<T>,
{
    log_start::<T>("signal", label_selector);
    let watch_stream = watcher(scope.into_api(client), watcher_config(label_selector))
        .applied_objects()
        .default_backoff();
    Ok(tokio::spawn(forward_watch(
        watch_stream,
        tx,
        applied_signal::<T>,
    )))
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
/// Pass [`Cluster`][crate::scope::Cluster] or [`Namespaced`][crate::scope::Namespaced] as the `scope` argument.
///
/// Returns a [`JoinHandle`] for the background task. Watch errors are logged
/// and retried with backoff; the task shuts down only when all receivers are
/// dropped (or the handle is aborted).
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
    Scope: ListScope<T>,
{
    log_start::<T>("object", label_selector);
    let watch_stream = watcher(scope.into_api(client), watcher_config(label_selector))
        .applied_objects()
        .default_backoff();
    Ok(tokio::spawn(forward_watch(
        watch_stream,
        tx,
        applied_object::<T>,
    )))
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
/// Pass [`Cluster`][crate::scope::Cluster] or [`Namespaced`][crate::scope::Namespaced] as the `scope` argument.
///
/// Returns a [`JoinHandle`] for the background task. Watch errors are logged
/// and retried with backoff; the task shuts down only when all receivers are
/// dropped (or the handle is aborted).
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
    Scope: ListScope<T>,
{
    log_start::<T>("event", label_selector);
    let watch_stream =
        watcher(scope.into_api(client), watcher_config(label_selector)).default_backoff();
    Ok(tokio::spawn(forward_watch(
        watch_stream,
        tx,
        to_watch_event::<T>,
    )))
}
