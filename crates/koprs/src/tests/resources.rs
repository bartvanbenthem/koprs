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
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tokio::time::timeout;
    use tower_test::mock;

    // Pull in the functions under test.
    use crate::resources::{
        EnsureOutcome, apply_resource, delete_resource, ensure_namespace, ensure_resource, exists,
        get_resource, list_resource_names, list_resources_scoped, patch_annotations, patch_labels,
        remove_annotations, remove_labels, wait_for_condition, wait_for_resources,
    };
    use crate::scope::{Cluster, Namespaced};
    use kube::api::ListParams;

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
            assert!(
                uri.contains("/namespaces/my-ns/configmaps/my-cm"),
                "uri={uri}"
            );
            assert!(uri.contains("fieldManager=my-op"), "uri={uri}");
            send.send_response(json_response(configmap_json("my-cm", "my-ns")));
        });

        let result = apply_resource::<ConfigMap, _>(client, Namespaced("my-ns"), &cm, "my-op")
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
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        apply_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op")
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
            assert!(
                !uri.contains("namespaces"),
                "unexpected namespace segment in uri={uri}"
            );
            send.send_response(json_response(node_json("n1")));
        });

        apply_resource::<Node, _>(client, Cluster, &node, "op")
            .await
            .unwrap();
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
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/my-ns/configmaps/cm1")
            );
            // Kubernetes responds with the deleted object or a Status 200.
            send.send_response(json_response(configmap_json("cm1", "my-ns")));
        });

        let deleted = delete_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "cm1")
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

        let deleted = delete_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "missing")
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

        let result = delete_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "cm1").await;
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
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let ok = delete_resource::<ConfigMap, _>(client, Namespaced("ns1"), "cm1")
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

        let ok = delete_resource::<Node, _>(client, Cluster, "n1")
            .await
            .unwrap();
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

        let ok = delete_resource::<Node, _>(client, Cluster, "ghost")
            .await
            .unwrap();
        assert!(!ok);
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // get_resource (generic, scope-dispatched)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_resource_namespaced_returns_some_when_found() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/my-ns/configmaps/cm1"),
            );
            send.send_response(json_response(configmap_json("cm1", "my-ns")));
        });

        let result = get_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "cm1")
            .await
            .unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().metadata.name.as_deref(), Some("cm1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_resource_returns_none_on_404() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());
        });

        let result = get_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "missing")
            .await
            .unwrap();
        assert!(result.is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_resource_propagates_non_404_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = get_resource::<ConfigMap, _>(client, Namespaced("my-ns"), "cm1").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // get_namespaced_resource / get_cluster_resource
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn get_namespaced_resource_sends_get_to_correct_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1"),
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let result = get_resource::<ConfigMap, _>(client, Namespaced("ns1"), "cm1")
            .await
            .unwrap();
        assert!(result.is_some());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_cluster_resource_sends_get_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json("n1")));
        });

        let result = get_resource::<Node, _>(client, Cluster, "n1")
            .await
            .unwrap();
        assert!(result.is_some());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_cluster_resource_returns_none_on_404() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());
        });

        let result = get_resource::<Node, _>(client, Cluster, "ghost")
            .await
            .unwrap();
        assert!(result.is_none());
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

        let list = list_resources_scoped::<ConfigMap, _>(client, Cluster, Default::default())
            .await
            .unwrap();
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

        let list = list_resources_scoped::<ConfigMap, _>(client, Cluster, Default::default())
            .await
            .unwrap();
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
                uri.contains("labelSelector=app%3Dmy-op")
                    || uri.contains("labelSelector=app=my-op"),
                "uri={uri}"
            );
            send.send_response(json_response(single_configmap_list(
                "cm-labeled",
                "default",
            )));
        });

        let list = list_resources_scoped::<ConfigMap, _>(
            client,
            Cluster,
            ListParams::default().labels("app=my-op"),
        )
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
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/prod/configmaps")
            );
            send.send_response(json_response(single_configmap_list("cm-prod", "prod")));
        });

        let list =
            list_resources_scoped::<ConfigMap, _>(client, Namespaced("prod"), Default::default())
                .await
                .unwrap();
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("cm-prod"));
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // list_namespaced_resources_by_label
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_namespaced_resources_by_label_scopes_to_namespace_and_label() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/prod/configmaps"), "uri={uri}");
            assert!(
                uri.contains("labelSelector=app%3Dmy-op")
                    || uri.contains("labelSelector=app=my-op"),
                "uri={uri}"
            );
            send.send_response(json_response(single_configmap_list("cm-prod", "prod")));
        });

        let list = list_resources_scoped::<ConfigMap, _>(
            client,
            Namespaced("prod"),
            ListParams::default().labels("app=my-op"),
        )
        .await
        .unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("cm-prod"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_namespaced_resources_by_label_returns_empty_list() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(empty_configmap_list()));
        });

        let list = list_resources_scoped::<ConfigMap, _>(
            client,
            Namespaced("prod"),
            ListParams::default().labels("app=my-op"),
        )
        .await
        .unwrap();
        assert!(list.items.is_empty());
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

        let names = list_resource_names::<ConfigMap>(client, "app=op")
            .await
            .unwrap();
        assert_eq!(
            names,
            HashSet::from(["alpha".to_string(), "beta".to_string()])
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_resource_names_returns_empty_set_when_no_resources() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(empty_configmap_list()));
        });

        let names = list_resource_names::<ConfigMap>(client, "app=op")
            .await
            .unwrap();
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

        let items = wait_for_resources::<ConfigMap, _>(
            client,
            Namespaced("ns1"),
            Duration::from_millis(10),
        )
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
            wait_for_resources::<ConfigMap, _>(
                client,
                Namespaced("ns1"),
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

        let items = wait_for_resources::<Node, _>(client, Cluster, Duration::from_millis(10))
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
            wait_for_resources::<Node, _>(client, Cluster, Duration::from_millis(10)),
        )
        .await
        .expect("timed out")
        .unwrap();

        assert_eq!(items.len(), 1);
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

        let result =
            list_resources_scoped::<ConfigMap, _>(client, Cluster, Default::default()).await;
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

        let result = apply_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Shared helper — read request body as JSON
    // -----------------------------------------------------------------------

    async fn read_body_json(req: http::Request<kube::client::Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // patch_labels
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_labels_namespaced_sends_merge_patch_with_labels() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            let body = read_body_json(req).await;
            assert_eq!(
                body["metadata"]["labels"]["app.kubernetes.io/managed-by"],
                "my-op"
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_labels::<ConfigMap, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            &[("app.kubernetes.io/managed-by", "my-op")],
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_labels_cluster_sends_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["labels"]["env"], "prod");
            send.send_response(json_response(node_json("n1")));
        });

        patch_labels::<Node, _>(client, Cluster, "n1", &[("env", "prod")])
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_labels_sends_multiple_labels_in_one_patch() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["labels"]["a"], "1");
            assert_eq!(body["metadata"]["labels"]["b"], "2");
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_labels::<ConfigMap, _>(client, Namespaced("ns1"), "cm1", &[("a", "1"), ("b", "2")])
            .await
            .unwrap();
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // patch_annotations
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_annotations_namespaced_sends_merge_patch_with_annotations() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["annotations"]["my-op/synced"], "true");
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_annotations::<ConfigMap, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            &[("my-op/synced", "true")],
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_annotations_cluster_sends_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["annotations"]["my-op/version"], "v1");
            send.send_response(json_response(node_json("n1")));
        });

        patch_annotations::<Node, _>(client, Cluster, "n1", &[("my-op/version", "v1")])
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_annotations_body_is_nested_under_metadata() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            // Confirm no labels key leaks into annotations patch
            assert!(body["metadata"]["labels"].is_null());
            assert_eq!(body["metadata"]["annotations"]["k"], "v");
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_annotations::<ConfigMap, _>(client, Namespaced("ns1"), "cm1", &[("k", "v")])
            .await
            .unwrap();
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // ensure_resource
    // -----------------------------------------------------------------------

    // Helper: configmap JSON with a specific resourceVersion.
    fn configmap_json_rv(name: &str, namespace: &str, rv: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace, "resourceVersion": rv }
        })
    }

    fn node_json_rv(name: &str, rv: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": name, "resourceVersion": rv }
        })
    }

    #[tokio::test]
    async fn ensure_resource_returns_created_when_resource_does_not_exist() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            // GET → 404 (resource absent)
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(not_found_response());

            // SSA PATCH → returns the newly-created object
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            send.send_response(json_response(configmap_json_rv("cm1", "ns1", "1")));
        });

        let outcome = ensure_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op")
            .await
            .unwrap();
        assert!(matches!(outcome, EnsureOutcome::Created(_)));
        assert!(outcome.was_changed());
        assert_eq!(
            outcome.into_resource().metadata.name.as_deref(),
            Some("cm1")
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ensure_resource_returns_updated_when_resource_version_changed() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            // GET → existing resource at rv "1"
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(json_response(configmap_json_rv("cm1", "ns1", "1")));

            // SSA → server applied a change, rv advances to "2"
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            send.send_response(json_response(configmap_json_rv("cm1", "ns1", "2")));
        });

        let outcome = ensure_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op")
            .await
            .unwrap();
        assert!(matches!(outcome, EnsureOutcome::Updated(_)));
        assert!(outcome.was_changed());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ensure_resource_returns_unchanged_when_resource_version_same() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            // GET → rv "1"
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json_rv("cm1", "ns1", "1")));

            // SSA → no change, rv stays "1"
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json_rv("cm1", "ns1", "1")));
        });

        let outcome = ensure_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op")
            .await
            .unwrap();
        assert!(matches!(outcome, EnsureOutcome::Unchanged(_)));
        assert!(!outcome.was_changed());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ensure_resource_propagates_get_error() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            // GET → 500; no PATCH should follow
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = ensure_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ensure_resource_propagates_apply_error_after_get() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            // GET → 404 (absent)
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());

            // SSA → 500
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = ensure_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // ensure_namespaced_resource / ensure_cluster_resource
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn ensure_namespaced_resource_sends_get_then_patch_to_correct_uri() {
        let (client, mut handle) = mock_client();
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            send.send_response(not_found_response());

            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            send.send_response(json_response(configmap_json_rv("cm1", "ns1", "1")));
        });

        let outcome = ensure_resource::<ConfigMap, _>(client, Namespaced("ns1"), &cm, "op")
            .await
            .unwrap();
        assert!(matches!(outcome, EnsureOutcome::Created(_)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn ensure_cluster_resource_sends_get_and_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();
        let node = serde_json::from_value::<Node>(node_json("n1")).unwrap();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json_rv("n1", "1")));

            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            // same rv → unchanged
            send.send_response(json_response(node_json_rv("n1", "1")));
        });

        let outcome = ensure_resource::<Node, _>(client, Cluster, &node, "op")
            .await
            .unwrap();
        assert!(matches!(outcome, EnsureOutcome::Unchanged(_)));
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // EnsureOutcome helpers
    // -----------------------------------------------------------------------

    #[test]
    fn ensure_outcome_into_resource_unwraps_all_variants() {
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        let created = EnsureOutcome::Created(cm.clone());
        assert_eq!(
            created.into_resource().metadata.name.as_deref(),
            Some("cm1")
        );

        let updated = EnsureOutcome::Updated(cm.clone());
        assert_eq!(
            updated.into_resource().metadata.name.as_deref(),
            Some("cm1")
        );

        let unchanged = EnsureOutcome::Unchanged(cm);
        assert_eq!(
            unchanged.into_resource().metadata.name.as_deref(),
            Some("cm1")
        );
    }

    #[test]
    fn ensure_outcome_was_changed_reflects_variant() {
        let cm = serde_json::from_value::<ConfigMap>(configmap_json("cm1", "ns1")).unwrap();

        assert!(EnsureOutcome::<ConfigMap>::Created(cm.clone()).was_changed());
        assert!(EnsureOutcome::<ConfigMap>::Updated(cm.clone()).was_changed());
        assert!(!EnsureOutcome::<ConfigMap>::Unchanged(cm).was_changed());
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn configmap_json_with_label(
        name: &str,
        namespace: &str,
        key: &str,
        val: &str,
    ) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "resourceVersion": "2",
                "labels": { key: val }
            }
        })
    }

    // -----------------------------------------------------------------------
    // exists / exists_namespaced / exists_cluster
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn exists_namespaced_returns_true_when_resource_found() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let found = exists::<ConfigMap, _>(client, Namespaced("ns1"), "cm1")
            .await
            .unwrap();
        assert!(found);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn exists_namespaced_returns_false_on_404() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());
        });

        let found = exists::<ConfigMap, _>(client, Namespaced("ns1"), "missing")
            .await
            .unwrap();
        assert!(!found);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn exists_cluster_sends_get_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert_eq!(req.method(), http::Method::GET);
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json("n1")));
        });

        let found = exists::<Node, _>(client, Cluster, "n1").await.unwrap();
        assert!(found);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn exists_propagates_non_404_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = exists::<ConfigMap, _>(client, crate::scope::Namespaced("ns1"), "cm1").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // list_by_field / list_namespaced_by_field
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn list_by_field_appends_field_selector_to_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("fieldSelector=spec.nodeName%3Dmy-node")
                    || uri.contains("fieldSelector=spec.nodeName=my-node"),
                "uri={uri}"
            );
            send.send_response(json_response(single_node_list("n1")));
        });

        let list = list_resources_scoped::<Node, _>(
            client,
            Cluster,
            ListParams::default().fields("spec.nodeName=my-node"),
        )
        .await
        .unwrap();
        assert_eq!(list.items.len(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn list_namespaced_by_field_scopes_to_namespace_and_field() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/prod/configmaps"), "uri={uri}");
            assert!(
                uri.contains("fieldSelector=metadata.name%3Dcm1")
                    || uri.contains("fieldSelector=metadata.name=cm1"),
                "uri={uri}"
            );
            send.send_response(json_response(single_configmap_list("cm1", "prod")));
        });

        let list = list_resources_scoped::<ConfigMap, _>(
            client,
            Namespaced("prod"),
            ListParams::default().fields("metadata.name=cm1"),
        )
        .await
        .unwrap();
        assert_eq!(list.items.len(), 1);
        assert_eq!(list.items[0].metadata.name.as_deref(), Some("cm1"));
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // wait_for_condition
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn wait_for_condition_returns_immediately_when_predicate_true() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let cm = wait_for_condition::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            Duration::from_millis(10),
            |_| true,
        )
        .await
        .unwrap();

        assert_eq!(cm.metadata.name.as_deref(), Some("cm1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_condition_retries_until_predicate_satisfied() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // First GET: resource exists but has no labels — predicate fails.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json("cm1", "ns1")));

            // Second GET: resource has the "ready" label — predicate passes.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json_with_label(
                "cm1", "ns1", "ready", "true",
            )));
        });

        let cm = timeout(
            Duration::from_secs(5),
            wait_for_condition::<ConfigMap, _, _>(
                client,
                Namespaced("ns1"),
                "cm1",
                Duration::from_millis(10),
                |cm| {
                    cm.metadata
                        .labels
                        .as_ref()
                        .map_or(false, |l| l.contains_key("ready"))
                },
            ),
        )
        .await
        .expect("timed out")
        .unwrap();

        assert!(cm.metadata.labels.as_ref().unwrap().contains_key("ready"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_condition_retries_when_resource_not_found() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // First GET: 404 — resource does not exist yet.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());

            // Second GET: resource appears and predicate passes.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let cm = timeout(
            Duration::from_secs(5),
            wait_for_condition::<ConfigMap, _, _>(
                client,
                Namespaced("ns1"),
                "cm1",
                Duration::from_millis(10),
                |_| true,
            ),
        )
        .await
        .expect("timed out")
        .unwrap();

        assert_eq!(cm.metadata.name.as_deref(), Some("cm1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_condition_cluster_sends_get_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert_eq!(req.method(), http::Method::GET);
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json("n1")));
        });

        let node = wait_for_condition::<Node, _, _>(
            client,
            Cluster,
            "n1",
            Duration::from_millis(10),
            |_| true,
        )
        .await
        .unwrap();

        assert_eq!(node.metadata.name.as_deref(), Some("n1"));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn wait_for_condition_generic_verifies_scope_dispatch() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        wait_for_condition::<ConfigMap, _, _>(
            client,
            crate::scope::Namespaced("ns1"),
            "cm1",
            Duration::from_millis(10),
            |_| true,
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // remove_labels
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn remove_labels_namespaced_sends_null_patch_for_key() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            let body = read_body_json(req).await;
            assert!(body["metadata"]["labels"]["stale-label"].is_null());
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        remove_labels::<ConfigMap, _>(client, Namespaced("ns1"), "cm1", &["stale-label"])
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_labels_cluster_sends_null_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            let body = read_body_json(req).await;
            assert!(body["metadata"]["labels"]["env"].is_null());
            send.send_response(json_response(node_json("n1")));
        });

        remove_labels::<Node, _>(client, Cluster, "n1", &["env"])
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_labels_sends_null_for_multiple_keys() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert!(body["metadata"]["labels"]["a"].is_null());
            assert!(body["metadata"]["labels"]["b"].is_null());
            let labels = body["metadata"]["labels"].as_object().unwrap();
            assert_eq!(labels.len(), 2, "patch must contain exactly the two keys");
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        remove_labels::<ConfigMap, _>(client, crate::scope::Namespaced("ns1"), "cm1", &["a", "b"])
            .await
            .unwrap();
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // remove_annotations
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn remove_annotations_namespaced_sends_null_patch_for_key() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/cm1")
            );
            let body = read_body_json(req).await;
            assert!(body["metadata"]["annotations"]["my-op/last-synced"].is_null());
            // must not bleed into the labels map
            assert!(body["metadata"]["labels"].is_null());
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        remove_annotations::<ConfigMap, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            &["my-op/last-synced"],
        )
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_annotations_cluster_sends_null_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            let body = read_body_json(req).await;
            assert!(body["metadata"]["annotations"]["my-op/version"].is_null());
            send.send_response(json_response(node_json("n1")));
        });

        remove_annotations::<Node, _>(client, Cluster, "n1", &["my-op/version"])
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_annotations_sends_null_for_multiple_keys() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert!(body["metadata"]["annotations"]["x"].is_null());
            assert!(body["metadata"]["annotations"]["y"].is_null());
            let annotations = body["metadata"]["annotations"].as_object().unwrap();
            assert_eq!(
                annotations.len(),
                2,
                "patch must contain exactly the two keys"
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        remove_annotations::<ConfigMap, _>(
            client,
            crate::scope::Namespaced("ns1"),
            "cm1",
            &["x", "y"],
        )
        .await
        .unwrap();
        server.await.unwrap();
    }
}
