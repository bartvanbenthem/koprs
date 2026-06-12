use std::fmt;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{info, warn};

use crate::error::Result;

// ---------------------------------------------------------------------------
// ExternalEvent
// ---------------------------------------------------------------------------

/// A change event produced by an [`ExternalSource`] on each poll.
///
/// The source implementation is responsible for tracking state between calls
/// and classifying events correctly.
#[derive(Debug, Clone)]
pub enum ExternalEvent<T> {
    /// An item was observed for the first time.
    Added(T),
    /// An item that was previously observed has changed.
    Modified(T),
    /// An item that was previously observed is no longer present.
    Removed(T),
}

// ---------------------------------------------------------------------------
// ExternalSource
// ---------------------------------------------------------------------------

/// A source that can be polled for changes.
///
/// Implementations maintain their own state between calls so they can
/// distinguish [`ExternalEvent::Added`], [`ExternalEvent::Modified`], and
/// [`ExternalEvent::Removed`] events.
///
/// The returned [`BoxFuture`] must be `Send` — all built-in sources satisfy
/// this requirement.
///
/// # Examples
///
/// ```
/// use futures::future::BoxFuture;
/// use koprs_external::error::Result;
/// use koprs_external::watcher::{ExternalEvent, ExternalSource};
///
/// struct AlwaysTicks;
///
/// impl ExternalSource for AlwaysTicks {
///     type Item = String;
///
///     fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<String>>>> {
///         Box::pin(async move {
///             Ok(vec![ExternalEvent::Added("tick".to_string())])
///         })
///     }
///
///     fn name(&self) -> &str { "always-ticks" }
/// }
/// ```
pub trait ExternalSource: Send + 'static {
    /// The item type produced by this source.
    type Item: Send + Clone + fmt::Debug + 'static;

    /// Poll the external source and return any change events since the last
    /// call.
    ///
    /// Implementations track state internally so that repeated calls to
    /// `poll` produce accurate `Added`, `Modified`, and `Removed` events
    /// without requiring the caller to diff results.
    fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<Self::Item>>>>;

    /// A human-readable identifier for this source, used in log output.
    fn name(&self) -> &str;
}

// ---------------------------------------------------------------------------
// watch_external
// ---------------------------------------------------------------------------

/// Spawn a background task that polls `source` on every `interval` tick and
/// forwards each [`ExternalEvent`] to `tx`.
///
/// The task shuts down automatically when all receivers are dropped or when
/// the returned [`JoinHandle`] is aborted.
///
/// Poll errors are logged as warnings and do not stop the watcher; the next
/// tick triggers a fresh poll attempt.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use koprs_external::watcher::{watch_external, ExternalSource};
/// use tokio::sync::mpsc;
///
/// # async fn example<S: ExternalSource>(source: S) {
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_external(source, Duration::from_secs(30), tx);
///
/// while let Some(event) = rx.recv().await {
///     println!("event: {:?}", event);
/// }
/// # }
/// ```
pub fn watch_external<S>(
    source: S,
    interval: Duration,
    tx: mpsc::Sender<ExternalEvent<S::Item>>,
) -> JoinHandle<()>
where
    S: ExternalSource,
{
    tokio::task::spawn(async move {
        let mut source = source;
        let name = source.name().to_owned();
        let mut ticker = time::interval(interval);
        ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

        info!(source = %name, "External watcher started");

        loop {
            ticker.tick().await;
            match source.poll().await {
                Ok(events) => {
                    for event in events {
                        if tx.send(event).await.is_err() {
                            info!(source = %name, "Receiver dropped; stopping external watcher");
                            return;
                        }
                    }
                }
                Err(e) => {
                    warn!(source = %name, error = %e, "Poll failed; retrying on next tick");
                }
            }
        }
    })
}
