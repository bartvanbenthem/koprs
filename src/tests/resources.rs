// src/tests/resources.rs
//
// Unit tests for the `resources` module.
//
// Strategy
// --------
// Every test spins up a `tower_test::mock::pair` — a (Client, Handle) tuple
// where the handle lets us intercept HTTP requests and inject hand-crafted
// JSON responses, exactly as the real Kubernetes API server would send them.
//
// We never touch a real cluster: all assertions are on the *call site* (which
// method was called, what URI was requested) and on the *return value* (the
// deserialized object our code hands back to the caller).
//
// Test resource
// -------------
// `ConfigMap` (namespaced) and `Node` (cluster-scoped) are used throughout
// because they ship with `k8s-openapi` and carry no derive macros of our own.

#[cfg(test)]
mod resources_tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::client::Body;
    use kube::Client;
    use serde_json::json;
    use tokio::time::timeout;
    use tower_test::mock;

    // Pull in the functions under test.
    use crate::resources::{
        apply_cluster_resource, apply_namespaced_resource, apply_resource, delete_cluster_resource,
        delete_namespaced_resource, delete_resource, ensure_namespace, fetch_and_write_to_file,
        list_namespaced_resources, list_resource_names, list_resources, list_resources_by_label,
        wait_for_resources_cluster, wait_for_resources_namespaced,
    };
    use crate::scope::{Cluster, Namespaced};

    // -----------------------------------------------------------------------
    // Test harness helpers
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    /// Create a mock (Client, Handle) pair.
    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        let client = Client::new(svc, "default");
        (client, handle)
    }

    /// Build a `200 OK` response carrying `body` as JSON bytes.
    fn json_response(body: serde_json::Value) -> Response<Body> {
        let bytes = serde_json::to_vec(&body).unwrap();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    /// Build a `404 Not Found` response with a standard Kubernetes Status body.
    fn not_found_response() -> Response<Body> {
        let body = json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "NotFound",
            "code": 404
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    /// Build a `500 Internal Server Error` response.
    fn server_error_response() -> Response<Body> {
        let body = json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "InternalError",
            "code": 500
        });
        let bytes = serde_json::to_vec(&body).unwrap();
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    /// Minimal `ConfigMap` JSON that kube can deserialise.
    fn configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace, "resourceVersion": "1" }
        })
    }

    /// Minimal `Node` JSON (cluster-scoped).
    fn node_json(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": name, "resourceVersion": "1" }
        })
    }

    /// Minimal `Namespace` JSON.
    fn namespace_json(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": { "name": name, "resourceVersion": "1" }
        })
    }

    /// A `ConfigMapList` JSON containing zero items.
    fn empty_configmap_list() -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": { "resourceVersion": "1" },
            "items": []
        })
    }

    /// A `ConfigMapList` JSON containing one item.
    fn single_configmap_list(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": { "resourceVersion": "1" },
            "items": [ configmap_json(name, namespace) ]
        })
    }

    /// A `NodeList` JSON containing one item.
    fn single_node_list(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "NodeList",
            "metadata": { "resourceVersion": "1" },
            "items": [ node_json(name) ]
        })
    }

    // -----------------------------------------------------------------------
    // ensure_namespace
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ensure_namespace_sends_ssa_patch_and_returns_namespace() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            // SSA is always a PATCH to /api/v1/namespaces/<name>?fieldManager=…&force=true
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/namespaces/my-ns"), "uri={uri}");
            assert!(uri.contains("fieldManager=my-op"), "uri={uri}");
            send.send_response(json_response(namespace_json("my-ns")));
        });

        let result = ensure_namespace(client, "my-ns", "my-op").await.unwrap();
        assert_eq!(result.metadata.name.unwrap(), "my-ns");
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // apply_resource  (generic, scope-dispatched)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn apply_resource_namespaced_sends_patch_and_returns_resource() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("my-cm", "my-ns")).unwrap();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/my-ns/configmaps/my-cm"), "uri={uri}");
            assert!(uri.contains("fieldManager=my-op"), "uri={uri}");
            send.send_response(json_response(configmap_json("my-cm", "my-ns")));
        });

        let result =
            apply_resource::<ConfigMap, _>(client, Namespaced("my-ns"), &cm, "my-op")
                .await
                .unwrap();
        assert_eq!(result.metadata.name.unwrap(), "my-cm");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn apply_resource_cluster_scoped_uses_all_api() {
        let (client, mut handle) = mock_client();
        let node = serde_json::from_value::<Node>(node_json("my-node")).unwrap();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            // cluster-scoped resource — no /namespaces/ segment
            assert!(uri.contains("/api/v1/nodes/my-node"), "uri={uri}");
            send.send_response(json_response(node_json("my-node")));
        });

        let result = apply_resource::<Node, _>(client, Cluster, &node, "my-op")
            .await
            .unwrap();
        assert_eq!(result.metadata.name.unwrap(), "my-node");
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // apply_namespaced_resource / apply_cluster_resource (convenience wrappers)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn apply_namespaced_resource_is_equivalent_to_generic_form() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(req.uri().to_string().contains("/namespaces/ns1/configmaps/cm1"));
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        apply_namespaced_resource::<ConfigMap>(client, "ns1", &cm, "op")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn apply_cluster_resource_sends_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();
        let node = serde_json::from_value::<Node>(node_json("n1")).unwrap();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "unexpected namespace segment in uri={uri}");
            send.send_response(json_response(node_json("n1")));
        });

        apply_cluster_resource::<Node>(client, &node, "op").await.unwrap();
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // delete_resource (generic, scope-dispatched)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn delete_resource_namespaced_returns_true_on_success() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            assert!(req.uri().to_string().contains("/namespaces/my-ns/configmaps/cm1"));
            // Kubernetes responds with the deleted object or a Status 200.
            send.send_response(json_response(configmap_json("cm1", "my-ns")));
        });

        let deleted =
            delete_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "cm1")
                .await
                .unwrap();
        assert!(deleted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delete_resource_returns_false_when_404() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());
        });

        let deleted =
            delete_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "missing")
                .await
                .unwrap();
        assert!(!deleted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delete_resource_propagates_non_404_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            delete_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "cm1").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // delete_namespaced_resource / delete_cluster_resource
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn delete_namespaced_resource_sends_delete_to_correct_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            assert!(req
                .uri()
                .to_string()
                .contains("/namespaces/ns1/configmaps/cm1"));
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let ok = delete_namespaced_resource::<ConfigMap>(client, "ns1", "cm1")
            .await
            .unwrap();
        assert!(ok);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delete_cluster_resource_sends_delete_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json("n1")));
        });

        let ok = delete_cluster_resource::<Node>(client, "n1").await.unwrap();
        assert!(ok);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delete_cluster_resource_returns_false_when_404() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());
        });

        let ok = delete_cluster_resource::<Node>(client, "ghost").await.unwrap();
        assert!(!ok);
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // list_resources
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_resources_returns_items_from_api() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            assert!(req.uri().to_string().contains("/api/v1/configmaps"));
            send.send_response(json_response(single_configmap_list("cm1", "default")));
        });

        let list = list_resources::<ConfigMap>(client).await.unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("cm1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_resources_returns_empty_list() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(empty_configmap_list()));
        });

        let list = list_resources::<ConfigMap>(client).await.unwrap();
        assert!(list.items.is_empty());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // list_resources_by_label
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_resources_by_label_appends_label_selector_to_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            // kube encodes the label selector as a query parameter
            assert!(
                uri.contains("labelSelector=app%3Dmy-op") || uri.contains("labelSelector=app=my-op"),
                "uri={uri}"
            );
            send.send_response(json_response(single_configmap_list("cm-labeled", "default")));
        });

        let list =
            list_resources_by_label::<ConfigMap>(client, "app=my-op")
                .await
                .unwrap();
        assert_eq!(list.items.len(), 1);
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // list_namespaced_resources
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_namespaced_resources_scopes_request_to_namespace() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(req.uri().to_string().contains("/namespaces/prod/configmaps"));
            send.send_response(json_response(single_configmap_list("cm-prod", "prod")));
        });

        let list = list_namespaced_resources::<ConfigMap>(client, "prod")
            .await
            .unwrap();
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("cm-prod"));
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // list_resource_names
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_resource_names_returns_hashset_of_names() {
        let (client, mut handle) = mock_client();

        let body = json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": {},
            "items": [
                configmap_json("alpha", "default"),
                configmap_json("beta",  "default"),
            ]
        });

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(body));
        });

        let names =
            list_resource_names::<ConfigMap>(client, "app=op").await.unwrap();
        assert_eq!(names, HashSet::from(["alpha".to_string(), "beta".to_string()]));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_resource_names_returns_empty_set_when_no_resources() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(empty_configmap_list()));
        });

        let names =
            list_resource_names::<ConfigMap>(client, "app=op").await.unwrap();
        assert!(names.is_empty());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // wait_for_resources_namespaced
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wait_for_resources_namespaced_returns_immediately_when_items_exist() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(single_configmap_list("cm1", "ns1")));
        });

        let items =
            wait_for_resources_namespaced::<ConfigMap>(client, "ns1", Duration::from_millis(10))
                .await
                .unwrap();
        assert_eq!(items.len(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_resources_namespaced_retries_until_resources_appear() {
        let (client, mut handle) = mock_client();

        // First call: empty list → should retry.
        // Second call: one item → should return.
        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(empty_configmap_list()));

            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(single_configmap_list("cm1", "ns1")));
        });

        // Use a tiny interval so the test is fast.
        let items = timeout(
            Duration::from_secs(5),
            wait_for_resources_namespaced::<ConfigMap>(
                client,
                "ns1",
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("timed out waiting for resources")
        .unwrap();

        assert_eq!(items.len(), 1);
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // wait_for_resources_cluster
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wait_for_resources_cluster_returns_when_items_appear() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(single_node_list("node1")));
        });

        let items =
            wait_for_resources_cluster::<Node>(client, Duration::from_millis(10))
                .await
                .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].metadata.name.as_deref(), Some("node1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_resources_cluster_retries_on_404() {
        // Simulates a CRD not yet installed (404) followed by a successful list.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // First call: 404 (CRD missing)
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());

            // Second call: resource exists
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(single_node_list("node1")));
        });

        let items = timeout(
            Duration::from_secs(5),
            wait_for_resources_cluster::<Node>(client, Duration::from_millis(10)),
        )
        .await
        .expect("timed out")
        .unwrap();

        assert_eq!(items.len(), 1);
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // fetch_and_write_to_file
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn fetch_and_write_to_file_creates_valid_json_file() {
        let (client, mut handle) = mock_client();
        let tmp = tempfile::tempdir().unwrap();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(single_configmap_list("cm1", "default")));
        });

        fetch_and_write_to_file::<ConfigMap, _>(client, tmp.path(), "out.json")
            .await
            .unwrap();

        let content = std::fs::read_to_string(tmp.path().join("out.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        let items = parsed.as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["metadata"]["name"], "cm1");

        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_and_write_to_file_writes_empty_array_when_no_resources() {
        let (client, mut handle) = mock_client();
        let tmp = tempfile::tempdir().unwrap();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(empty_configmap_list()));
        });

        fetch_and_write_to_file::<ConfigMap, _>(client, tmp.path(), "empty.json")
            .await
            .unwrap();

        let content = std::fs::read_to_string(tmp.path().join("empty.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert_eq!(parsed, json!([]));

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Error propagation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_resources_propagates_server_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = list_resources::<ConfigMap>(client).await;
        assert!(result.is_err(), "expected Err, got Ok");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn apply_resource_propagates_server_errors() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            apply_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op").await;
        assert!(result.is_err());
        server.await.unwrap();
    }
}
