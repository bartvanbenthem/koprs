// src/tests/watcher.rs
//
// Testing strategy
// ----------------
// kube_runtime::watcher drives a two-phase protocol under the hood:
//
//   1. LIST   GET /api/v1/.../configmaps?watch=false&...
//   2. WATCH  GET /api/v1/.../configmaps?watch=true&resourceVersion=...
//
// The mock handle must serve both responses before the background task
// produces any signals on the mpsc channel. The WATCH response body is a
// newline-delimited stream of JSON `WatchEvent` objects — each event on its
// own line. Sending an ADDED event through the watch is what causes the
// watcher task to call `tx.send(())`.
//
// Because the watcher task runs in the background, every test that expects a
// signal uses `tokio::time::timeout` so a broken watcher cannot hang the
// suite indefinitely.

#[cfg(test)]
mod watcher_tests {
    use std::time::Duration;

    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::client::Body;
    use kube::Client;
    use serde_json::json;
    use tokio::sync::mpsc;
    use tokio::time::timeout;
    use tower_test::mock;

    use crate::scope::{Cluster, Namespaced};
    use crate::watcher::{
        watch, watch_cluster, watch_cluster_by_label, watch_namespaced, watch_namespaced_by_label,
    };

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (Client::new(svc, "default"), handle)
    }

    fn json_response(body: serde_json::Value) -> Response<Body> {
        let bytes = serde_json::to_vec(&body).unwrap();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // Protocol helpers
    // -----------------------------------------------------------------------

    /// A minimal ConfigMap JSON object, usable in list items and watch events.
    fn configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "resourceVersion": "100"
            }
        })
    }

    /// A minimal Node JSON object.
    fn node_json(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "resourceVersion": "100"
            }
        })
    }

    /// The initial LIST response kube_runtime expects before opening a watch.
    ///
    /// `items` populates the list. `resource_version` seeds the `?resourceVersion=`
    /// parameter on the subsequent WATCH request.
    fn list_response(kind: &str, items: Vec<serde_json::Value>) -> Response<Body> {
        let body = json!({
            "apiVersion": "v1",
            "kind": kind,
            "metadata": { "resourceVersion": "100" },
            "items": items
        });
        json_response(body)
    }

    /// A WATCH response body: a newline-delimited stream of `WatchEvent` JSON
    /// objects. kube_runtime reads this as a streaming response — each `\n`
    /// terminates one event.
    ///
    /// Sending an `ADDED` event for an object causes `applied_objects()` to
    /// yield it, which is what makes the watcher task call `tx.send(())`.
    fn watch_events_response(events: Vec<serde_json::Value>) -> Response<Body> {
        let ndjson = events
            .into_iter()
            .map(|e| serde_json::to_string(&e).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(ndjson.into_bytes()))
            .unwrap()
    }

    /// A single `ADDED` WatchEvent wrapping the given object.
    fn added_event(object: serde_json::Value) -> serde_json::Value {
        json!({ "type": "ADDED", "object": object })
    }

    /// A single `MODIFIED` WatchEvent wrapping the given object.
    fn modified_event(object: serde_json::Value) -> serde_json::Value {
        json!({ "type": "MODIFIED", "object": object })
    }

    /// Assert that `rx` receives at least one signal within 2 seconds.
    async fn expect_signal(rx: &mut mpsc::Receiver<()>) {
        timeout(Duration::from_secs(2), rx.recv())
            .await
            .expect("timed out waiting for watcher signal")
            .expect("channel closed before signal was received");
    }

    /// Assert that no signal arrives within 200 ms — used to verify the
    /// watcher did *not* fire for a DELETED event or a filtered resource.
    async fn expect_no_signal(rx: &mut mpsc::Receiver<()>) {
        let result = timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(
            result.is_err(),
            "expected no signal, but watcher sent one unexpectedly"
        );
    }

    // -----------------------------------------------------------------------
    // watch — LIST + WATCH protocol
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_issues_list_then_watch_requests_in_sequence() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            // 1. Initial LIST request.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/ns1/configmaps"),
                "expected namespaced configmap list uri, got: {uri}"
            );
            // watch=true must NOT be present on the list call
            assert!(
                !uri.contains("watch=true"),
                "list request must not have watch=true, got: {uri}"
            );
            send.send_response(list_response("ConfigMapList", vec![]));

            // 2. Long-poll WATCH request.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("watch=true"),
                "second request must be a watch, got: {uri}"
            );
            // Send an empty watch stream so the task completes cleanly.
            send.send_response(watch_events_response(vec![]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // watch — signal is sent for ADDED events
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_sends_signal_when_resource_is_added() {
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            // LIST — empty, just to seed resourceVersion
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("ConfigMapList", vec![]));

            // WATCH — one ADDED event
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![added_event(configmap_json(
                "cm1", "ns1",
            ))]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch — signal is sent for MODIFIED events
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_sends_signal_when_resource_is_modified() {
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![modified_event(configmap_json(
                "cm1", "ns1",
            ))]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch — DELETED events do not produce signals
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_does_not_send_signal_for_deleted_events() {
        // applied_objects() filters out DELETED events — only ADDED and
        // MODIFIED pass through. A DELETED event must not trigger tx.send(()).
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![json!({
                "type": "DELETED",
                "object": configmap_json("cm1", "ns1")
            })]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        expect_no_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch — existing resources in the LIST also trigger signals
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_sends_signal_for_resources_present_in_initial_list() {
        // kube_runtime's applied_objects() synthesises ADDED events for items
        // returned in the initial list, so they also trigger tx.send(()).
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            // LIST — already contains one ConfigMap
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response(
                "ConfigMapList",
                vec![configmap_json("existing", "ns1")],
            ));

            // WATCH — empty, nothing new
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch — multiple events produce one signal each
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_sends_one_signal_per_applied_event() {
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![
                added_event(configmap_json("cm1", "ns1")),
                added_event(configmap_json("cm2", "ns1")),
            ]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        // Two ADDED events → two signals
        expect_signal(&mut rx).await;
        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch — label selector is forwarded to the API server
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_with_label_selector_forwards_selector_on_list_request() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            // LIST — check label selector is present
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("labelSelector=app%3Dmy-op")
                    || uri.contains("labelSelector=app=my-op"),
                "expected labelSelector in list uri, got: {uri}"
            );
            send.send_response(list_response("ConfigMapList", vec![]));

            // WATCH — label selector should also be present
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("labelSelector=app%3Dmy-op")
                    || uri.contains("labelSelector=app=my-op"),
                "expected labelSelector in watch uri, got: {uri}"
            );
            send.send_response(watch_events_response(vec![]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), Some("app=my-op"), tx)
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn watch_without_label_selector_omits_label_selector_param() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                !uri.contains("labelSelector"),
                "expected no labelSelector in uri, got: {uri}"
            );
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // watch — cluster-scoped resources use Api::all (no namespace segment)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_cluster_scoped_list_uri_has_no_namespace_segment() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/api/v1/nodes"),
                "expected nodes list uri, got: {uri}"
            );
            assert!(
                !uri.contains("namespaces"),
                "cluster-scoped watch must not have namespace segment, got: {uri}"
            );
            send.send_response(list_response("NodeList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch::<Node, _>(client, Cluster, None, tx).await.unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn watch_cluster_scoped_sends_signal_on_added_event() {
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("NodeList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![added_event(node_json("n1"))]));
        });

        watch::<Node, _>(client, Cluster, None, tx).await.unwrap();

        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch_namespaced — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_namespaced_scopes_list_to_correct_namespace() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/prod/configmaps"),
                "expected prod namespace in uri, got: {uri}"
            );
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch_namespaced::<ConfigMap>(client, "prod", tx)
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn watch_namespaced_sends_signal_on_applied_event() {
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![added_event(configmap_json(
                "cm1", "prod",
            ))]));
        });

        watch_namespaced::<ConfigMap>(client, "prod", tx)
            .await
            .unwrap();

        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch_namespaced_by_label — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_namespaced_by_label_forwards_label_selector() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("labelSelector"),
                "expected labelSelector in uri, got: {uri}"
            );
            assert!(
                uri.contains("/namespaces/ns1/configmaps"),
                "expected ns1 in uri, got: {uri}"
            );
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch_namespaced_by_label::<ConfigMap>(client, "ns1", "app=my-op", tx)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // watch_cluster — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_cluster_uses_all_api_without_namespace_segment() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(!uri.contains("namespaces"), "uri={uri}");
            assert!(uri.contains("/api/v1/nodes"), "uri={uri}");
            send.send_response(list_response("NodeList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch_cluster::<Node>(client, tx).await.unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn watch_cluster_sends_signal_on_applied_event() {
        let (client, mut handle) = mock_client();
        let (tx, mut rx) = mpsc::channel(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("NodeList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![added_event(node_json("n1"))]));
        });

        watch_cluster::<Node>(client, tx).await.unwrap();

        expect_signal(&mut rx).await;
    }

    // -----------------------------------------------------------------------
    // watch_cluster_by_label — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watch_cluster_by_label_forwards_selector_without_namespace() {
        let (client, mut handle) = mock_client();
        let (tx, _rx) = mpsc::channel(16);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(!uri.contains("namespaces"), "uri={uri}");
            assert!(uri.contains("labelSelector"), "uri={uri}");
            send.send_response(list_response("NodeList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(watch_events_response(vec![]));
        });

        watch_cluster_by_label::<Node>(client, "app=my-op", tx)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // JoinHandle — task shuts down when receiver is dropped
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn watcher_task_shuts_down_when_all_receivers_are_dropped() {
        let (client, mut handle) = mock_client();
        let (tx, rx) = mpsc::channel::<()>(16);

        tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(list_response("ConfigMapList", vec![]));

            let (_req, send) = handle.next_request().await.unwrap();
            // Send a signal — but the receiver is already dropped by the time
            // the watcher task tries to send, so tx.send().ok() should swallow
            // the error rather than panic.
            send.send_response(watch_events_response(vec![added_event(configmap_json(
                "cm1", "ns1",
            ))]));
        });

        let handle = watch::<ConfigMap, _>(client, Namespaced("ns1"), None, tx)
            .await
            .unwrap();

        // Drop the receiver — the watcher task's tx.send().ok() must not panic.
        drop(rx);

        // The task should finish without panicking.
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("watcher task did not shut down within timeout")
            .expect("watcher task panicked");
    }
}