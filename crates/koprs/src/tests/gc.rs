// src/tests/gc.rs

#[cfg(test)]
mod gc_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::gc::gc_resources;
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

    fn not_found_response() -> Response<Body> {
        let body = json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Failure",
            "reason": "NotFound",
            "code": 404
        });
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
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

    // -----------------------------------------------------------------------
    // Fixture builders
    // -----------------------------------------------------------------------

    /// A ConfigMap without a deletionTimestamp — a normal live resource.
    fn configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "resourceVersion": "1"
            }
        })
    }

    /// A ConfigMap whose deletionTimestamp is set — it is already terminating.
    fn terminating_configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "resourceVersion": "1",
                "deletionTimestamp": "2024-01-01T00:00:00Z",
                "finalizers": ["some-op/cleanup"]
            }
        })
    }

    /// A Node (cluster-scoped) without a deletionTimestamp.
    fn node_json(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "resourceVersion": "1"
            }
        })
    }

    /// A ConfigMapList containing the given items.
    fn configmap_list(items: Vec<serde_json::Value>) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": { "resourceVersion": "1" },
            "items": items
        })
    }

    /// A NodeList containing the given items.
    fn node_list(items: Vec<serde_json::Value>) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "NodeList",
            "metadata": { "resourceVersion": "1" },
            "items": items
        })
    }

    /// An empty ConfigMapList.
    fn empty_configmap_list() -> serde_json::Value {
        configmap_list(vec![])
    }

    // -----------------------------------------------------------------------
    // Read the outgoing request body as JSON.
    // -----------------------------------------------------------------------

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // gc_resources — nothing to do (empty list)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_does_nothing_when_list_is_empty() {
        let (client, mut handle) = mock_client();

        // Only one request is expected: the initial list.
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(json_response(empty_configmap_list()));
            // No further requests should arrive — the handle is dropped here.
        });

        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| true)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — all resources are desired (no deletions)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_skips_resources_that_are_desired() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(json_response(configmap_list(vec![configmap_json(
                "cm-keep", "ns1",
            )])));
            // No DELETE or PATCH should follow — only one request total.
        });

        // Predicate always returns true → everything is desired.
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| true)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — orphaned resource is deleted
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_deletes_orphaned_resource_not_in_desired_set() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List call — returns one orphaned resource.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("labelSelector"),
                "expected label selector in list call, uri={uri}"
            );
            send.send_response(json_response(configmap_list(vec![configmap_json(
                "orphan", "ns1",
            )])));

            // 2. DELETE call for the orphaned resource.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/ns1/configmaps/orphan"),
                "uri={}",
                req.uri()
            );
            // Kubernetes returns the deleted object (or a Status) — we return the object.
            send.send_response(json_response(configmap_json("orphan", "ns1")));

            // 3. After deletion kube calls clear_finalizers (PATCH finalizers=null).
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let body = read_body_json(req).await;
            assert_eq!(
                body["metadata"]["finalizers"],
                serde_json::Value::Null,
                "clear_finalizers must set finalizers to null"
            );
            send.send_response(json_response(configmap_json("orphan", "ns1")));
        });

        // Predicate never matches "orphan" → it should be deleted.
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |r| {
            r.metadata.name.as_deref() != Some("orphan")
        })
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — multiple resources, mixed desired / orphaned
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_only_deletes_orphaned_resources_from_mixed_list() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List returns two resources: one desired, one orphaned.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_list(vec![
                configmap_json("keep", "ns1"),
                configmap_json("orphan", "ns1"),
            ])));

            // 2. DELETE for "orphan" only (no call for "keep").
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            assert!(
                req.uri().to_string().contains("orphan"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(configmap_json("orphan", "ns1")));

            // 3. clear_finalizers PATCH after delete.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json("orphan", "ns1")));
        });

        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |r| {
            r.metadata.name.as_deref() == Some("keep")
        })
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — terminating resource gets finalizers cleared, not deleted
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_clears_finalizers_on_terminating_resource_instead_of_deleting() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List returns a resource that is already terminating.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_list(vec![
                terminating_configmap_json("terminating", "ns1"),
            ])));

            // 2. Should go straight to a PATCH (clear_finalizers), skipping DELETE.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(
                req.method(),
                http::Method::PATCH,
                "expected PATCH to clear finalizers, not DELETE"
            );
            let uri = req.uri().to_string();
            assert!(uri.contains("terminating"), "uri={uri}");
            let body = read_body_json(req).await;
            assert_eq!(body["metadata"]["finalizers"], serde_json::Value::Null);
            send.send_response(json_response(terminating_configmap_json(
                "terminating",
                "ns1",
            )));
        });

        // Resource is not desired (would normally trigger deletion), but because
        // it has a deletionTimestamp the GC loop should only clear finalizers.
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — delete returns 404 (already gone), should not error
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_tolerates_404_on_delete_as_already_deleted() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_list(vec![configmap_json(
                "gone", "ns1",
            )])));

            // 2. DELETE → 404 (someone else already deleted it).
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            send.send_response(not_found_response());

            // No PATCH should follow — the 404 path skips clear_finalizers.
        });

        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap(); // must not return Err

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — non-404 delete error is propagated
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_propagates_non_404_delete_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_list(vec![configmap_json(
                "cm1", "ns1",
            )])));

            // 2. DELETE → 500
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false).await;
        assert!(result.is_err(), "expected Err on 500 delete, got Ok");

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_resources — list error is propagated
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_propagates_list_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false).await;
        assert!(result.is_err(), "expected Err on 500 list, got Ok");

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_cluster_resources — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_cluster_resources_lists_and_deletes_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List — cluster-scoped, no /namespaces/ in URI.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            let uri = req.uri().to_string();
            assert!(
                !uri.contains("namespaces"),
                "unexpected namespace in list uri={uri}"
            );
            send.send_response(json_response(node_list(vec![node_json("orphan-node")])));

            // 2. DELETE
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/orphan-node"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json("orphan-node")));

            // 3. clear_finalizers PATCH
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(node_json("orphan-node")));
        });

        gc_resources::<Node, _>(client, Cluster, "app=op", |_| false)
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_cluster_resources_skips_desired_nodes() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(node_list(vec![node_json("keep-node")])));
            // No further requests — the single desired node is skipped.
        });

        gc_resources::<Node, _>(client, Cluster, "app=op", |_| true)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // gc_namespaced_resources — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_namespaced_resources_lists_and_deletes_within_namespace() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List (gc_resources uses Api::all internally, then per-resource
            //    Api::namespaced — so the list URI uses the all-namespaces path).
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::GET);
            send.send_response(json_response(configmap_list(vec![configmap_json(
                "orphan", "prod",
            )])));

            // 2. DELETE via namespaced API.
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::DELETE);
            assert!(
                req.uri().to_string().contains("configmaps/orphan"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(configmap_json("orphan", "prod")));

            // 3. clear_finalizers PATCH.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json("orphan", "prod")));
        });

        gc_resources::<ConfigMap, _>(client, Namespaced("prod"), "app=op", |_| false)
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // clear_finalizers errors are silently swallowed
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_continues_when_clear_finalizers_patch_fails() {
        // After a successful delete, the clear_finalizers PATCH may fail (e.g.
        // the resource is already fully gone). The GC loop must swallow that
        // error and return Ok.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            // 1. List
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_list(vec![configmap_json(
                "orphan", "ns1",
            )])));

            // 2. DELETE succeeds.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json("orphan", "ns1")));

            // 3. PATCH (clear_finalizers) → 404, resource already gone.
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(not_found_response());
        });

        // Must not return Err even though the finalizer clear failed.
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();

        server.await.unwrap();
    }
}
