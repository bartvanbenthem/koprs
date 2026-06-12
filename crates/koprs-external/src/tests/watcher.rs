// src/tests/watcher.rs
//
// Testing strategy
// ----------------
// ExternalSource is tested via a SequenceSource that yields a fixed list of
// events on the first poll and nothing on subsequent ones. This lets us
// verify that watch_external correctly forwards events and shuts down when
// the receiver is dropped, without any real I/O.

#[cfg(test)]
mod watcher_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::future::BoxFuture;
    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use crate::error::Result;
    use crate::watcher::{ExternalEvent, ExternalSource, watch_external};

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
}
