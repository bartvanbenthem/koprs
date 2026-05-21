// src/tests/finalizers.rs

#[cfg(test)]
mod finalizers_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::client::Body;
    use kube::Client;
    use serde_json::json;
    use tower_test::mock;

    use crate::finalizers::{
        add_finalizer, add_finalizer_cluster, add_finalizer_namespaced, remove_finalizers,
        remove_finalizers_cluster, remove_finalizers_namespaced,
    };
    use crate::scope::{Cluster, Namespaced};

    // -----------------------------------------------------------------------
    // Harness — identical contract to tests/common.rs in the real project
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

    fn server_error_response() -> Response<Body> {
        let body = json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "InternalError",
            "code": 500
        });
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn configmap_json(name: &str, namespace: &str, finalizers: &[&str]) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "resourceVersion": "1",
                "finalizers": finalizers
            }
        })
    }

    fn node_json(name: &str, finalizers: &[&str]) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "resourceVersion": "1",
                "finalizers": finalizers
            }
        })
    }

    // -----------------------------------------------------------------------
    // Helpers that assert on the outgoing patch body
    // -----------------------------------------------------------------------

    /// Reads the request body bytes from a `Request<Body>` and parses as JSON.
    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // add_finalizer — generic, scope-dispatched
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_namespaced_sends_merge_patch_to_correct_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();

            // Must be a PATCH (strategic merge patch)
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/my-ns/configmaps/my-cm"),
                "uri={uri}"
            );

            let body = read_body_json(req).await;
            assert_eq!(
                body["metadata"]["finalizers"],
                json!(["my-op/cleanup"]),
                "patch body did not contain expected finalizer"
            );

            send.send_response(json_response(configmap_json(
                "my-cm",
                "my-ns",
                &["my-op/cleanup"],
            )));
        });

        let result = add_finalizer::<ConfigMap, _>(
            client,
            Namespaced("my-ns"),
            "my-cm",
            "my-op/cleanup",
        )
        .await
        .unwrap();

        assert_eq!(
            result.metadata.finalizers.as_deref(),
            Some(&["my-op/cleanup".to_string()][..])
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_cluster_scoped_sends_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/my-node"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "unexpected namespace in uri={uri}");

            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], json!(["my-op/cleanup"]));

            send.send_response(json_response(node_json("my-node", &["my-op/cleanup"])));
        });

        add_finalizer::<Node, _>(client, Cluster, "my-node", "my-op/cleanup")
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // add_finalizer — convenience wrappers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_namespaced_wrapper_delegates_correctly() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/ns1/configmaps/cm1"), "uri={uri}");

            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], json!(["op/fin"]));

            send.send_response(json_response(configmap_json("cm1", "ns1", &["op/fin"])));
        });

        add_finalizer_namespaced::<ConfigMap>(client, "ns1", "cm1", "op/fin")
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_cluster_wrapper_delegates_correctly() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");

            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], json!(["op/fin"]));

            send.send_response(json_response(node_json("n1", &["op/fin"])));
        });

        add_finalizer_cluster::<Node>(client, "n1", "op/fin")
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // remove_finalizers — generic, scope-dispatched
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn remove_finalizers_namespaced_patches_finalizers_to_null() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/my-ns/configmaps/my-cm"),
                "uri={uri}"
            );

            let body = read_body_json(req).await;
            // The patch must explicitly set finalizers to JSON null, not an
            // empty array — null is what actually clears the field in kube.
            assert_eq!(
                body["metadata"]["finalizers"],
                serde_json::Value::Null,
                "expected finalizers to be null in patch body"
            );

            // Simulate kube returning the object with no finalizers.
            send.send_response(json_response(configmap_json("my-cm", "my-ns", &[])));
        });

        let result =
            remove_finalizers::<ConfigMap, _>(client, Namespaced("my-ns"), "my-cm")
                .await
                .unwrap();

        assert!(
            result
                .metadata
                .finalizers
                .as_ref()
                .map_or(true, |f| f.is_empty()),
            "expected no finalizers on returned resource"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_finalizers_cluster_scoped_patches_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/my-node"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");

            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], serde_json::Value::Null);

            send.send_response(json_response(node_json("my-node", &[])));
        });

        remove_finalizers::<Node, _>(client, Cluster, "my-node")
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // remove_finalizers — convenience wrappers
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn remove_finalizers_namespaced_wrapper_delegates_correctly() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/ns1/configmaps/cm1"), "uri={uri}");

            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], serde_json::Value::Null);

            send.send_response(json_response(configmap_json("cm1", "ns1", &[])));
        });

        remove_finalizers_namespaced::<ConfigMap>(client, "ns1", "cm1")
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_finalizers_cluster_wrapper_delegates_correctly() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");

            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], serde_json::Value::Null);

            send.send_response(json_response(node_json("n1", &[])));
        });

        remove_finalizers_cluster::<Node>(client, "n1").await.unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Returned value reflects what the API server sends back
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_returns_resource_with_finalizer_from_server_response() {
        // The server is authoritative — we return whatever it sends back,
        // not what we patched. This test verifies we deserialise it correctly.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            // Server response includes two finalizers (pre-existing + new one).
            send.send_response(json_response(configmap_json(
                "cm1",
                "ns1",
                &["existing/fin", "my-op/cleanup"],
            )));
        });

        let result =
            add_finalizer_namespaced::<ConfigMap>(client, "ns1", "cm1", "my-op/cleanup")
                .await
                .unwrap();

        let fins = result.metadata.finalizers.unwrap();
        assert!(fins.contains(&"existing/fin".to_string()));
        assert!(fins.contains(&"my-op/cleanup".to_string()));
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Error propagation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_propagates_server_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            add_finalizer::<ConfigMap, _>(client, Namespaced("ns1"), "cm1", "op/fin").await;
        assert!(result.is_err(), "expected Err on 500, got Ok");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_finalizers_propagates_server_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            remove_finalizers::<ConfigMap, _>(client, Namespaced("ns1"), "cm1").await;
        assert!(result.is_err(), "expected Err on 500, got Ok");
        server.await.unwrap();
    }
}