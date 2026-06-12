// src/tests/store.rs
//
// Testing strategy
// ----------------
// object_store ships an InMemory backend that is always available without any
// feature flags. Using it lets us test the full ObjectStorePoller logic —
// Added, Modified, and Removed events — without AWS credentials, LocalStack,
// or any external service.

#[cfg(test)]
#[cfg(feature = "object-store")]
mod store_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use object_store::{ObjectStore, PutPayload, memory::InMemory, path::Path};
    use tokio::time::timeout;

    use crate::store::{ObjectStorePoller, StoredObject};
    use crate::watcher::{ExternalEvent, ExternalSource};

    fn in_memory_poller() -> (Arc<InMemory>, ObjectStorePoller) {
        let store = Arc::new(InMemory::new());
        let poller =
            ObjectStorePoller::new(Arc::clone(&store) as Arc<dyn object_store::ObjectStore>);
        (store, poller)
    }

    async fn put(store: &InMemory, key: &str, body: &str) {
        store
            .put(
                &Path::from(key),
                PutPayload::from(Bytes::from(body.to_string())),
            )
            .await
            .unwrap();
    }

    async fn delete(store: &InMemory, key: &str) {
        ObjectStore::delete(store, &Path::from(key)).await.unwrap();
    }

    async fn poll(poller: &mut ObjectStorePoller) -> Vec<ExternalEvent<StoredObject>> {
        timeout(Duration::from_secs(2), poller.poll())
            .await
            .expect("poll timed out")
            .expect("poll returned an error")
    }

    // -----------------------------------------------------------------------
    // StoredObject — basic properties
    // -----------------------------------------------------------------------

    #[test]
    fn stored_object_fields_are_accessible() {
        let obj = StoredObject {
            path: "data/file.json".to_string(),
            etag: Some("\"abc\"".to_string()),
            size: 128,
        };
        assert_eq!(obj.path, "data/file.json");
        assert_eq!(obj.size, 128);
    }

    #[test]
    fn stored_object_is_cloneable() {
        let obj = StoredObject {
            path: "k".to_string(),
            etag: None,
            size: 0,
        };
        let obj2 = obj.clone();
        assert_eq!(obj2.path, "k");
    }

    #[test]
    fn stored_object_is_debug_formattable() {
        let obj = StoredObject {
            path: "debug-key".to_string(),
            etag: None,
            size: 0,
        };
        assert!(format!("{obj:?}").contains("debug-key"));
    }

    // -----------------------------------------------------------------------
    // ObjectStorePoller — Added
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_added_for_new_objects() {
        let (store, mut poller) = in_memory_poller();
        put(&store, "config.json", r#"{"v":1}"#).await;

        let events = poll(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ExternalEvent::Added(o) if o.path == "config.json"));
    }

    #[tokio::test]
    async fn poll_emits_nothing_when_store_is_empty() {
        let (_, mut poller) = in_memory_poller();
        let events = poll(&mut poller).await;
        assert!(events.is_empty(), "expected no events on empty store");
    }

    #[tokio::test]
    async fn poll_emits_added_only_once_per_object() {
        let (store, mut poller) = in_memory_poller();
        put(&store, "file.txt", "hello").await;

        // First poll → Added
        let events = poll(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ExternalEvent::Added(_)));

        // Second poll (no change) → no events
        let events = poll(&mut poller).await;
        assert!(events.is_empty(), "Added should be reported exactly once");
    }

    // -----------------------------------------------------------------------
    // ObjectStorePoller — Modified
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_modified_when_object_content_changes() {
        let (store, mut poller) = in_memory_poller();
        put(&store, "config.json", "v1").await;

        // First poll → Added
        let events = poll(&mut poller).await;
        assert!(matches!(events[0], ExternalEvent::Added(_)));

        // Overwrite with new content (new ETag)
        put(&store, "config.json", "v2").await;

        // Second poll → Modified
        let events = poll(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ExternalEvent::Modified(o) if o.path == "config.json"),
            "expected Modified after content change, got {:?}",
            events[0]
        );
    }

    // -----------------------------------------------------------------------
    // ObjectStorePoller — Removed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_removed_when_object_is_deleted() {
        let (store, mut poller) = in_memory_poller();
        put(&store, "gone.txt", "bye").await;

        // First poll → Added
        let events = poll(&mut poller).await;
        assert!(matches!(events[0], ExternalEvent::Added(_)));

        // Delete the object
        delete(&store, "gone.txt").await;

        // Second poll → Removed
        let events = poll(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ExternalEvent::Removed(o) if o.path == "gone.txt"),
            "expected Removed after delete, got {:?}",
            events[0]
        );

        // Third poll → already gone, no further event
        let events = poll(&mut poller).await;
        assert!(events.is_empty(), "Removed should be reported exactly once");
    }

    // -----------------------------------------------------------------------
    // ObjectStorePoller — mixed events in one poll
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_reports_mixed_events_in_a_single_tick() {
        let (store, mut poller) = in_memory_poller();
        put(&store, "a.txt", "a").await;
        put(&store, "b.txt", "b").await;

        // Seed state — Added for both
        let events = poll(&mut poller).await;
        assert_eq!(events.len(), 2);

        // Change a, delete b, add c
        put(&store, "a.txt", "a-updated").await;
        delete(&store, "b.txt").await;
        put(&store, "c.txt", "c").await;

        let events = poll(&mut poller).await;
        // Sort by path for deterministic assertions
        let mut by_path: Vec<_> = events
            .iter()
            .map(|e| match e {
                ExternalEvent::Added(o) => ("added", o.path.clone()),
                ExternalEvent::Modified(o) => ("modified", o.path.clone()),
                ExternalEvent::Removed(o) => ("removed", o.path.clone()),
            })
            .collect();
        by_path.sort_by(|a, b| a.1.cmp(&b.1));

        assert_eq!(by_path.len(), 3);
        assert_eq!(by_path[0], ("modified", "a.txt".to_string()));
        assert_eq!(by_path[1], ("removed", "b.txt".to_string()));
        assert_eq!(by_path[2], ("added", "c.txt".to_string()));
    }

    // -----------------------------------------------------------------------
    // ObjectStorePoller — prefix filtering
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn with_prefix_filters_listing_to_matching_paths() {
        let store = Arc::new(InMemory::new());
        put(&store, "data/file.txt", "in-prefix").await;
        put(&store, "other/file.txt", "outside-prefix").await;

        let mut poller =
            ObjectStorePoller::new(Arc::clone(&store) as Arc<dyn object_store::ObjectStore>)
                .with_prefix("data/");

        let events = poll(&mut poller).await;
        assert_eq!(
            events.len(),
            1,
            "only data/ prefix objects should be returned"
        );
        if let ExternalEvent::Added(ref obj) = events[0] {
            assert!(
                obj.path.starts_with("data/"),
                "expected data/ path, got {}",
                obj.path
            );
        } else {
            panic!("expected Added event");
        }
    }

    // -----------------------------------------------------------------------
    // ObjectStorePoller::with_name
    // -----------------------------------------------------------------------

    #[test]
    fn with_name_overrides_default_display_name() {
        let store = Arc::new(InMemory::new());
        let poller =
            ObjectStorePoller::new(Arc::clone(&store) as Arc<dyn object_store::ObjectStore>)
                .with_name("my-store");
        assert_eq!(poller.name(), "my-store");
    }
}
