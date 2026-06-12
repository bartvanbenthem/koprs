// src/tests/watcher.rs
//
// Testing strategy
// ----------------
// ExternalSource is tested via a SequenceSource that yields a fixed list of
// events on the first poll and nothing on subsequent ones. FlakySource
// simulates transient failures before eventually succeeding, exercising the
// exponential backoff path. Both run fully in-process — no I/O required.

#[cfg(test)]
mod watcher_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use crate::error::{ExternalError, Result};
    use crate::watcher::{
        ExternalEvent, ExternalSource, WatchConfig, watch_external, watch_external_with_config,
    };

    // -----------------------------------------------------------------------
    // SequenceSource — yields a fixed set of events then goes quiet
    // -----------------------------------------------------------------------

    struct SequenceSource {
        events: Arc<Mutex<Vec<ExternalEvent<String>>>>,
        name: String,
    }

    impl SequenceSource {
        fn new(name: &str, events: Vec<ExternalEvent<String>>) -> Self {
            Self {
                events: Arc::new(Mutex::new(events)),
                name: name.to_string(),
            }
        }
    }

    impl ExternalSource for SequenceSource {
        type Item = String;

        fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<String>>>> {
            let events = self.events.clone();
            Box::pin(async move {
                let mut guard = events.lock().unwrap();
                Ok(guard.drain(..).collect())
            })
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    // -----------------------------------------------------------------------
    // FlakySource — fails N times then succeeds
    // -----------------------------------------------------------------------

    struct FlakySource {
        failures_remaining: u32,
    }

    impl ExternalSource for FlakySource {
        type Item = String;

        fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<String>>>> {
            let result = if self.failures_remaining > 0 {
                self.failures_remaining -= 1;
                Err(ExternalError::Internal("deliberate failure".to_string()))
            } else {
                Ok(vec![ExternalEvent::Added("recovered".to_string())])
            };
            Box::pin(async move { result })
        }

        fn name(&self) -> &str {
            "flaky"
        }
    }

    // -----------------------------------------------------------------------
    // ExternalEvent — basic properties
    // -----------------------------------------------------------------------

    #[test]
    fn external_event_added_is_debug_formattable() {
        let e: ExternalEvent<u32> = ExternalEvent::Added(42);
        assert!(format!("{e:?}").contains("Added"));
    }

    #[test]
    fn external_event_modified_is_debug_formattable() {
        let e: ExternalEvent<u32> = ExternalEvent::Modified(7);
        assert!(format!("{e:?}").contains("Modified"));
    }

    #[test]
    fn external_event_removed_is_debug_formattable() {
        let e: ExternalEvent<u32> = ExternalEvent::Removed(0);
        assert!(format!("{e:?}").contains("Removed"));
    }

    #[test]
    fn external_event_clone_preserves_variant_and_value() {
        let e = ExternalEvent::Modified("hello".to_string());
        let e2 = e.clone();
        assert!(matches!(e2, ExternalEvent::Modified(ref s) if s == "hello"));
    }

    // -----------------------------------------------------------------------
    // WatchConfig — construction and field values
    // -----------------------------------------------------------------------

    #[test]
    fn watch_config_default_max_backoff_is_32_times_interval() {
        let interval = Duration::from_millis(100);
        let config = WatchConfig::new(interval);
        assert_eq!(config.interval, interval);
        assert_eq!(config.max_backoff, interval * 32);
    }

    #[test]
    fn watch_config_with_max_backoff_overrides_default() {
        let config =
            WatchConfig::new(Duration::from_secs(30)).with_max_backoff(Duration::from_secs(600));
        assert_eq!(config.max_backoff, Duration::from_secs(600));
    }

    // -----------------------------------------------------------------------
    // watch_external — events reach the channel
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_external_forwards_added_event_to_channel() {
        let source = SequenceSource::new(
            "added-test",
            vec![ExternalEvent::Added("first".to_string())],
        );

        let (tx, mut rx) = mpsc::channel(16);
        let _handle = watch_external(source, Duration::from_millis(1), tx);

        let ev = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for event")
            .expect("channel closed before event was received");

        assert!(matches!(ev, ExternalEvent::Added(ref s) if s == "first"));
    }

    #[tokio::test]
    async fn watch_external_forwards_multiple_events_in_order() {
        let source = SequenceSource::new(
            "multi-test",
            vec![
                ExternalEvent::Added("a".to_string()),
                ExternalEvent::Modified("b".to_string()),
                ExternalEvent::Removed("c".to_string()),
            ],
        );

        let (tx, mut rx) = mpsc::channel(16);
        let _handle = watch_external(source, Duration::from_millis(1), tx);

        let ev1 = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out on first event")
            .expect("channel closed");
        assert!(matches!(ev1, ExternalEvent::Added(ref s) if s == "a"));

        let ev2 = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out on second event")
            .expect("channel closed");
        assert!(matches!(ev2, ExternalEvent::Modified(ref s) if s == "b"));

        let ev3 = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out on third event")
            .expect("channel closed");
        assert!(matches!(ev3, ExternalEvent::Removed(ref s) if s == "c"));
    }

    // -----------------------------------------------------------------------
    // watch_external — task shuts down when receiver is dropped
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_external_shuts_down_when_receiver_is_dropped() {
        let source =
            SequenceSource::new("drop-test", vec![ExternalEvent::Added("item".to_string())]);

        let (tx, rx) = mpsc::channel::<ExternalEvent<String>>(16);
        let handle = watch_external(source, Duration::from_millis(1), tx);

        drop(rx);

        timeout(Duration::from_secs(2), handle)
            .await
            .expect("watcher task did not shut down after receiver was dropped")
            .expect("watcher task panicked");
    }

    // -----------------------------------------------------------------------
    // watch_external — exponential backoff on errors
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_external_recovers_after_consecutive_errors() {
        // Fails 3 times then succeeds; all waits are 1–8 ms so recovery
        // arrives well within the 2-second timeout.
        let source = FlakySource {
            failures_remaining: 3,
        };
        let (tx, mut rx) = mpsc::channel(16);
        let _handle = watch_external(source, Duration::from_millis(1), tx);

        let ev = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("watcher did not recover after 3 errors — timed out")
            .expect("channel closed before recovery");

        assert!(matches!(ev, ExternalEvent::Added(ref s) if s == "recovered"));
    }

    #[tokio::test]
    async fn watch_external_does_not_stop_on_errors() {
        let source = FlakySource {
            failures_remaining: 5,
        };
        let (tx, mut rx) = mpsc::channel(16);
        let _handle = watch_external(source, Duration::from_millis(1), tx);

        let ev = timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("watcher terminated after 5 errors instead of recovering")
            .expect("channel closed");

        assert!(matches!(ev, ExternalEvent::Added(_)));
    }

    #[tokio::test]
    async fn watch_external_with_config_recovers_after_errors() {
        let source = FlakySource {
            failures_remaining: 2,
        };
        let config =
            WatchConfig::new(Duration::from_millis(1)).with_max_backoff(Duration::from_millis(10));
        let (tx, mut rx) = mpsc::channel(16);
        let _handle = watch_external_with_config(source, config, tx);

        let ev = timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for recovery")
            .expect("channel closed");

        assert!(matches!(ev, ExternalEvent::Added(_)));
    }
}
