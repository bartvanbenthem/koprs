// src/tests/events.rs

#[cfg(test)]
mod events_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::events::{EventType, record_event};

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
            .status(StatusCode::CREATED)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    fn configmap(name: &str, namespace: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": "abc-123",
                "resourceVersion": "1"
            }
        }))
        .unwrap()
    }

    fn event_response(namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "events.k8s.io/v1",
            "kind": "Event",
            "metadata": { "name": "my-cm.abc", "namespace": namespace },
            "eventTime": "2024-01-01T00:00:00.000000Z",
            "reportingController": "my-operator",
            "reportingInstance": "pod-1"
        })
    }

    // -----------------------------------------------------------------------
    // record_event — sends POST to the correct events API endpoint
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn record_event_posts_to_events_api() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::POST);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/apis/events.k8s.io/v1/namespaces/my-ns/events"),
                "unexpected uri: {uri}"
            );
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "All good",
            "my-operator",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_body_contains_reason_and_action() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["reason"], "Synced");
            assert_eq!(body["action"], "Sync");
            assert_eq!(body["note"], "All resources up to date");
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "All resources up to date",
            "my-operator",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_normal_sets_type_normal() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["type"], "Normal");
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "ok",
            "my-op",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_warning_sets_type_warning() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["type"], "Warning");
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Warning,
            "Sync",
            "SyncFailed",
            "error applying child",
            "my-op",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_body_contains_regarding_reference() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["regarding"]["kind"], "ConfigMap");
            assert_eq!(body["regarding"]["name"], "my-cm");
            assert_eq!(body["regarding"]["namespace"], "my-ns");
            assert_eq!(body["regarding"]["uid"], "abc-123");
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "ok",
            "my-op",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_body_sets_reporting_controller() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["reportingController"], "my-operator");
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "ok",
            "my-operator",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_metadata_namespace_matches_resource() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "ops-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["namespace"], "ops-ns");
            send.send_response(json_response(event_response("ops-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "ok",
            "my-op",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn record_event_event_time_is_present() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns");

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert!(
                !body["eventTime"].is_null(),
                "eventTime must be present in the event body"
            );
            send.send_response(json_response(event_response("my-ns")));
        });

        record_event(
            client,
            &cm,
            EventType::Normal,
            "Sync",
            "Synced",
            "ok",
            "my-op",
        )
        .await
        .unwrap();
        server.await.unwrap();
    }
}
