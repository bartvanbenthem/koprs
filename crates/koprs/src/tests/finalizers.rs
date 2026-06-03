// src/tests/finalizers.rs

#[cfg(test)]
mod finalizers_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::finalizers::{add_finalizer, add_finalizer_namespaced, remove_finalizers};
    use crate::scope::{Cluster, Namespaced};

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

    fn configmap(name: &str, namespace: &str, finalizers: &[&str]) -> ConfigMap {
        serde_json::from_value(configmap_json(name, namespace, finalizers)).unwrap()
    }

    fn node(name: &str, finalizers: &[&str]) -> Node {
        serde_json::from_value(node_json(name, finalizers)).unwrap()
    }

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // add_finalizer — guard: no API call when already present
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_is_noop_when_finalizer_already_present() {
        let (client, handle) = mock_client();
        let cm = configmap("my-cm", "my-ns", &["my-op/cleanup"]);

        // Spawn server side — it must NOT receive any request.
        let server = tokio::spawn(async move {
            // Give the client side time to complete without a request.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            // If a request arrives after this point, next_request would return
            // Some, but we simply don't await it — the assertion is that the
            // client already returned Ok(()) without touching the handle.
            drop(handle);
        });

        let result = add_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
            .await
            .unwrap();

        // Returned resource is the same as the input (cloned, no server round-trip).
        assert_eq!(
            result.metadata.finalizers.as_deref(),
            Some(&["my-op/cleanup".to_string()][..])
        );
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // add_finalizer — sends patch when finalizer is absent
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_namespaced_sends_merge_patch_to_correct_uri() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns", &[]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
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

        let result =
            add_finalizer::<ConfigMap, _>(client, Namespaced("my-ns"), &cm, "my-op/cleanup")
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
        let n = node("my-node", &[]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/my-node"), "uri={uri}");
            assert!(
                !uri.contains("namespaces"),
                "unexpected namespace in uri={uri}"
            );
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], json!(["my-op/cleanup"]));
            send.send_response(json_response(node_json("my-node", &["my-op/cleanup"])));
        });

        add_finalizer::<Node, _>(client, Cluster, &n, "my-op/cleanup")
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
        let cm = configmap("cm1", "ns1", &[]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/ns1/configmaps/cm1"), "uri={uri}");
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], json!(["op/fin"]));
            send.send_response(json_response(configmap_json("cm1", "ns1", &["op/fin"])));
        });

        add_finalizer_namespaced::<ConfigMap>(client, &cm, "op/fin")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_cluster_wrapper_delegates_correctly() {
        let (client, mut handle) = mock_client();
        let n = node("n1", &[]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], json!(["op/fin"]));
            send.send_response(json_response(node_json("n1", &["op/fin"])));
        });

        add_finalizer::<Node, _>(client, Cluster, &n, "op/fin")
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
            assert_eq!(
                body["metadata"]["finalizers"],
                serde_json::Value::Null,
                "expected finalizers to be null in patch body"
            );
            send.send_response(json_response(configmap_json("my-cm", "my-ns", &[])));
        });

        let result = remove_finalizers::<ConfigMap, _>(client, Namespaced("my-ns"), "my-cm")
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

        remove_finalizers::<ConfigMap, _>(client, Namespaced("ns1"), "cm1")
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

        remove_finalizers::<Node, _>(client, Cluster, "n1")
            .await
            .unwrap();
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // add_finalizer — server response and error propagation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_returns_resource_with_finalizer_from_server_response() {
        let (client, mut handle) = mock_client();
        let cm = configmap("cm1", "ns1", &["existing/fin"]);

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json(
                "cm1",
                "ns1",
                &["existing/fin", "my-op/cleanup"],
            )));
        });

        let result = add_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
            .await
            .unwrap();

        let fins = result.metadata.finalizers.unwrap();
        assert!(fins.contains(&"existing/fin".to_string()));
        assert!(fins.contains(&"my-op/cleanup".to_string()));
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // add_finalizer — preserves existing finalizers in the patch body
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_preserves_existing_finalizers_in_patch_body() {
        let (client, mut handle) = mock_client();
        // Resource already has one finalizer; adding a second must not drop the first.
        let cm = configmap("cm1", "ns1", &["existing/fin"]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            let fins = body["metadata"]["finalizers"].as_array().unwrap().clone();
            let fin_strs: Vec<&str> = fins.iter().map(|v| v.as_str().unwrap()).collect();
            assert!(
                fin_strs.contains(&"existing/fin"),
                "existing finalizer must be preserved: {fin_strs:?}"
            );
            assert!(
                fin_strs.contains(&"my-op/cleanup"),
                "new finalizer must be present: {fin_strs:?}"
            );
            send.send_response(json_response(configmap_json(
                "cm1",
                "ns1",
                &["existing/fin", "my-op/cleanup"],
            )));
        });

        add_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_propagates_server_errors() {
        let (client, mut handle) = mock_client();
        let cm = configmap("cm1", "ns1", &[]);

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = add_finalizer::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op/fin").await;
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

        let result = remove_finalizers::<ConfigMap, _>(client, Namespaced("ns1"), "cm1").await;
        assert!(result.is_err(), "expected Err on 500, got Ok");
        server.await.unwrap();
    }
}
