use std::fmt;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
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
// WatchConfig
// ---------------------------------------------------------------------------

/// Configuration for [`watch_external_with_config`].
///
/// Controls the base polling interval and the ceiling applied during
/// exponential backoff when consecutive poll failures occur.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use koprs_external::watcher::WatchConfig;
///
/// // Poll every 30 s; back off up to 10 min on errors.
/// let config = WatchConfig::new(Duration::from_secs(30))
///     .with_max_backoff(Duration::from_secs(600));
/// ```
#[derive(Debug, Clone)]
pub struct WatchConfig {
    /// Base polling interval used after a successful poll.
    pub interval: Duration,
    /// Maximum wait during backoff (default: `interval × 32`).
    pub max_backoff: Duration,
}

impl WatchConfig {
    /// Create a new configuration with the given base interval.
    ///
    /// `max_backoff` defaults to `interval × 32` (five doublings from the
    /// base). Override with [`with_max_backoff`][Self::with_max_backoff].
    pub fn new(interval: Duration) -> Self {
        Self {
            max_backoff: interval.saturating_mul(32),
            interval,
        }
    }

    /// Set an explicit upper bound for backoff waits.
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

fn backoff_wait(base: Duration, consecutive_errors: u32, max: Duration) -> Duration {
    // 2^min(n, 30) keeps the shift within u32; saturating_mul caps at u64::MAX.
    let factor = 1u32
        .checked_shl(consecutive_errors.min(30))
        .unwrap_or(u32::MAX);
    base.saturating_mul(factor).min(max)
}

// ---------------------------------------------------------------------------
// watch_external_with_config
// ---------------------------------------------------------------------------

/// Spawn a background task that polls `source` according to `config` and
/// forwards each [`ExternalEvent`] to `tx`.
///
/// **Exponential backoff**: consecutive poll failures increase the retry wait
/// exponentially — starting at `config.interval`, doubling each time, capped
/// at `config.max_backoff`. The wait resets to `config.interval` on the next
/// successful poll.
///
/// The task shuts down automatically when all receivers are dropped or when
/// the returned [`JoinHandle`] is aborted.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use koprs_external::watcher::{WatchConfig, watch_external_with_config, ExternalSource};
/// use tokio::sync::mpsc;
///
/// # async fn example<S: ExternalSource>(source: S) {
/// let config = WatchConfig::new(Duration::from_secs(30))
///     .with_max_backoff(Duration::from_secs(300));
/// let (tx, mut rx) = mpsc::channel(16);
/// let _handle = watch_external_with_config(source, config, tx);
///
/// while let Some(event) = rx.recv().await {
///     println!("event: {:?}", event);
/// }
/// # }
/// ```
pub fn watch_external_with_config<S>(
    source: S,
    config: WatchConfig,
    tx: mpsc::Sender<ExternalEvent<S::Item>>,
) -> JoinHandle<()>
where
    S: ExternalSource,
{
    tokio::task::spawn(async move {
        let mut source = source;
        let name = source.name().to_owned();
        let mut consecutive_errors: u32 = 0;

        info!(source = %name, "External watcher started");

        loop {
            match source.poll().await {
                Ok(events) => {
                    consecutive_errors = 0;
                    for event in events {
                        if tx.send(event).await.is_err() {
                            info!(source = %name, "Receiver dropped; stopping external watcher");
                            return;
                        }
                    }
                    sleep(config.interval).await;
                }
                Err(e) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    let wait =
                        backoff_wait(config.interval, consecutive_errors, config.max_backoff);
                    warn!(
                        source = %name,
                        error = %e,
                        attempt = consecutive_errors,
                        ?wait,
                        "Poll failed; retrying after backoff"
                    );
                    sleep(wait).await;
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// watch_external  (convenience wrapper)
// ---------------------------------------------------------------------------

/// Spawn a background task that polls `source` every `interval` and forwards
/// each [`ExternalEvent`] to `tx`.
///
/// This is a convenience wrapper around [`watch_external_with_config`] using
/// [`WatchConfig::new`] with default backoff settings (`max_backoff =
/// interval × 32`). Call [`watch_external_with_config`] directly to set a
/// custom ceiling.
///
/// **Exponential backoff**: on consecutive poll errors the retry wait doubles
/// starting from `interval`, capped at `interval × 32`. It resets to
/// `interval` on the next success.
///
/// The task shuts down automatically when all receivers are dropped or when
/// the returned [`JoinHandle`] is aborted.
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
    watch_external_with_config(source, WatchConfig::new(interval), tx)
}
