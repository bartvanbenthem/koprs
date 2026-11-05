// src/tests/gc.rs

#[cfg(test)]
mod gc_tests {
    use http::{Method, Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::client::Body;
    use kube::{Client, ResourceExt};
    use serde_json::json;
    use tower_test::mock;

    use crate::error::KubeGenericError;
    use crate::gc::gc_resources;
    use crate::scope::{AllNamespaces, Cluster, Namespaced};

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (Client::new(svc, "default"), handle)
    }

    fn json_response(body: serde_json::Value) -> Response<Body> {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn status_response(code: u16, reason: &str) -> Response<Body> {
        let body = json!({
            "apiVersion": "v1", "kind": "Status", "status": "Failure",
            "reason": reason, "code": code
        });
        Response::builder()
            .status(StatusCode::from_u16(code).unwrap())
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn deleted_status() -> Response<Body> {
        json_response(
            json!({ "apiVersion": "v1", "kind": "Status", "status": "Success", "code": 200 }),
        )
    }

    fn configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {
                "name": name,
                "namespace": namespace,
                "uid": format!("uid-{namespace}-{name}"),
                "resourceVersion": "1"
            }
        })
    }

    fn terminating_configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        let mut cm = configmap_json(name, namespace);
        cm["metadata"]["deletionTimestamp"] = json!("2024-01-01T00:00:00Z");
        cm["metadata"]["finalizers"] = json!(["some-op/cleanup"]);
        cm
    }

    fn node_json(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": name, "uid": format!("uid-{name}"), "resourceVersion": "1" }
        })
    }

    fn list(kind: &str, items: Vec<serde_json::Value>) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": kind,
            "metadata": { "resourceVersion": "1" },
            "items": items
        })
    }

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    async fn assert_no_more_requests(handle: &mut MockHandle) {
        let next =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle.next_request()).await;
        // Timed out (nothing sent) or the client is gone (`None`) — either way no request.
        assert!(matches!(next, Err(_) | Ok(None)), "unexpected API request");
    }

    // -----------------------------------------------------------------------
    // Guard rails
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_rejects_empty_label_selector() {
        let (client, mut handle) = mock_client();
        let err = gc_resources::<ConfigMap, _>(client, Namespaced("ns"), "  ", |_| false)
            .await
            .unwrap_err();
        assert!(matches!(err, KubeGenericError::Config(_)), "got {err:?}");
        assert_no_more_requests(&mut handle).await;
    }

    // -----------------------------------------------------------------------
    // Listing respects the scope
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_namespaced_lists_only_within_that_namespace() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/namespaces/ns1/configmaps"), "uri={uri}");
            assert!(uri.contains("labelSelector=app%3Dop"), "uri={uri}");
            send.send_response(json_response(list("ConfigMapList", vec![])));
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_does_nothing_when_list_is_empty() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list("ConfigMapList", vec![])));
            assert_no_more_requests(&mut handle).await;
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns"), "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Deletion
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_skips_resources_that_are_desired() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![configmap_json("keep", "ns1")],
            )));
            assert_no_more_requests(&mut handle).await;
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| true)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_deletes_orphan_with_uid_precondition_and_background_propagation() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![configmap_json("orphan", "ns1")],
            )));

            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::DELETE);
            assert!(
                req.uri()
                    .path()
                    .ends_with("/namespaces/ns1/configmaps/orphan"),
                "uri={}",
                req.uri()
            );
            let body = read_body_json(req).await;
            assert_eq!(body["propagationPolicy"], "Background");
            assert_eq!(body["preconditions"]["uid"], "uid-ns1-orphan");
            send.send_response(deleted_status());

            // No follow-up finalizer patch.
            assert_no_more_requests(&mut handle).await;
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_all_namespaces_deletes_each_object_in_its_own_namespace() {
        // Two orphans with the same name in different namespaces: each delete
        // must target the namespace the object was found in.
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                !req.uri().path().contains("/namespaces/"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![
                    configmap_json("dup", "team-a"),
                    configmap_json("dup", "team-b"),
                ],
            )));
            for ns in ["team-a", "team-b"] {
                let (req, send) = handle.next_request().await.unwrap();
                assert_eq!(req.method(), Method::DELETE);
                assert!(
                    req.uri()
                        .path()
                        .ends_with(&format!("/namespaces/{ns}/configmaps/dup")),
                    "uri={}",
                    req.uri()
                );
                send.send_response(deleted_status());
            }
        });
        gc_resources::<ConfigMap, _>(client, AllNamespaces, "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_only_deletes_orphaned_resources_from_mixed_list() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![
                    configmap_json("keep", "ns1"),
                    configmap_json("orphan", "ns1"),
                ],
            )));
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri().path().ends_with("/configmaps/orphan"),
                "uri={}",
                req.uri()
            );
            send.send_response(deleted_status());
            assert_no_more_requests(&mut handle).await;
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |cm| {
            cm.name_any() == "keep"
        })
        .await
        .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_leaves_terminating_resources_and_their_finalizers_alone() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![terminating_configmap_json("dying", "ns1")],
            )));
            // No DELETE and, crucially, no finalizer-clearing PATCH.
            assert_no_more_requests(&mut handle).await;
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_tolerates_404_on_delete_as_already_deleted() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![configmap_json("gone", "ns1")],
            )));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(404, "NotFound"));
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_tolerates_409_when_object_was_replaced_since_listing() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![configmap_json("recreated", "ns1")],
            )));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(409, "Conflict"));
        });
        gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_propagates_non_404_delete_errors() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(list(
                "ConfigMapList",
                vec![configmap_json("orphan", "ns1")],
            )));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
        });
        let result =
            gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false).await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn gc_propagates_list_errors() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
        });
        let result =
            gc_resources::<ConfigMap, _>(client, Namespaced("ns1"), "app=op", |_| false).await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Cluster-scoped resources
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn gc_cluster_resources_lists_and_deletes_without_namespace_segment() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                !req.uri().to_string().contains("namespaces"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(list("NodeList", vec![node_json("old")])));
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::DELETE);
            assert!(
                req.uri().path().ends_with("/api/v1/nodes/old"),
                "uri={}",
                req.uri()
            );
            send.send_response(deleted_status());
        });
        gc_resources::<Node, _>(client, Cluster, "app=op", |_| false)
            .await
            .unwrap();
        server.await.unwrap();
    }
}
