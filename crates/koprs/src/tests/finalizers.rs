// src/tests/finalizers.rs

#[cfg(test)]
#[allow(deprecated)] // `remove_finalizers` is still tested until it is removed.
mod finalizers_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::finalizers::{
        add_finalizer, add_finalizer_namespaced, add_finalizer_patch, remove_finalizer,
        remove_finalizer_namespaced, remove_finalizer_patch, remove_finalizers,
    };
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
            "metadata": { "name": name, "resourceVersion": "1", "finalizers": finalizers }
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

    fn content_type(req: &Request<Body>) -> String {
        req.headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    /// Assert the handle receives no request within a short window.
    async fn assert_no_request(mut handle: MockHandle) {
        let next =
            tokio::time::timeout(std::time::Duration::from_millis(50), handle.next_request()).await;
        // Timed out (nothing sent) or the client is gone (`None`) — either way no request.
        assert!(matches!(next, Err(_) | Ok(None)), "unexpected API request");
    }

    // -----------------------------------------------------------------------
    // Patch construction — pure functions
    // -----------------------------------------------------------------------

    #[test]
    fn add_patch_for_empty_list_tests_null_then_adds_array() {
        let patch = serde_json::to_value(add_finalizer_patch(&[], "op/f").unwrap()).unwrap();
        assert_eq!(
            patch,
            json!([
                { "op": "test", "path": "/metadata/finalizers", "value": null },
                { "op": "add", "path": "/metadata/finalizers", "value": ["op/f"] },
            ])
        );
    }

    #[test]
    fn add_patch_for_existing_list_tests_whole_list_then_appends() {
        let current = vec!["other/f".to_string()];
        let patch = serde_json::to_value(add_finalizer_patch(&current, "op/f").unwrap()).unwrap();
        assert_eq!(
            patch,
            json!([
                { "op": "test", "path": "/metadata/finalizers", "value": ["other/f"] },
                { "op": "add", "path": "/metadata/finalizers/-", "value": "op/f" },
            ])
        );
    }

    #[test]
    fn remove_patch_tests_entry_at_index_before_removing_it() {
        let patch = serde_json::to_value(remove_finalizer_patch(2, "op/f").unwrap()).unwrap();
        assert_eq!(
            patch,
            json!([
                { "op": "test", "path": "/metadata/finalizers/2", "value": "op/f" },
                { "op": "remove", "path": "/metadata/finalizers/2" },
            ])
        );
    }

    // -----------------------------------------------------------------------
    // add_finalizer
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn add_finalizer_is_noop_when_finalizer_already_present() {
        let (client, handle) = mock_client();
        let cm = configmap("my-cm", "my-ns", &["my-op/cleanup"]);
        let result = add_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
            .await
            .unwrap();
        assert_eq!(
            result.metadata.finalizers.as_deref(),
            Some(&["my-op/cleanup".to_string()][..])
        );
        assert_no_request(handle).await;
    }

    #[tokio::test]
    async fn add_finalizer_namespaced_sends_guarded_json_patch() {
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns", &["other-controller/keep"]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            assert!(
                req.uri()
                    .path()
                    .ends_with("/namespaces/my-ns/configmaps/my-cm"),
                "uri={}",
                req.uri()
            );
            assert_eq!(content_type(&req), "application/json-patch+json");
            let body = read_body_json(req).await;
            // The other controller's finalizer is only *tested*, never rewritten.
            assert_eq!(body[0]["op"], "test");
            assert_eq!(body[0]["value"], json!(["other-controller/keep"]));
            assert_eq!(
                body[1],
                json!({ "op": "add", "path": "/metadata/finalizers/-", "value": "my-op/cleanup" })
            );
            send.send_response(json_response(configmap_json(
                "my-cm",
                "my-ns",
                &["other-controller/keep", "my-op/cleanup"],
            )));
        });

        let result = add_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
            .await
            .unwrap();
        assert_eq!(result.metadata.finalizers.unwrap().len(), 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_cluster_scoped_sends_patch_without_namespace_segment() {
        let (client, mut handle) = mock_client();
        let n = node("my-node", &[]);

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                !req.uri().path().contains("namespaces"),
                "uri={}",
                req.uri()
            );
            assert!(req.uri().path().ends_with("/nodes/my-node"));
            let body = read_body_json(req).await;
            assert_eq!(body[0]["value"], json!(null));
            send.send_response(json_response(node_json("my-node", &["my-op/cleanup"])));
        });

        add_finalizer::<Node, _>(client, Cluster, &n, "my-op/cleanup")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_surfaces_precondition_failure() {
        // A concurrent writer changed the finalizer list: the API server
        // rejects the `test` op with 422 and we must return an error rather
        // than overwrite the other controller's change.
        let (client, mut handle) = mock_client();
        let cm = configmap("my-cm", "my-ns", &[]);

        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(422, "Invalid"));
        });

        let result =
            add_finalizer::<ConfigMap, _>(client, Namespaced("my-ns"), &cm, "my-op/f").await;
        assert!(result.is_err());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_finalizer_errors_when_name_missing() {
        let (client, handle) = mock_client();
        let cm = ConfigMap::default();
        let err = add_finalizer::<ConfigMap, _>(client, Namespaced("ns"), &cm, "op/f")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::KubeGenericError::MissingMetadata(_)
        ));
        assert_no_request(handle).await;
    }

    #[tokio::test]
    async fn add_finalizer_namespaced_errors_when_namespace_missing() {
        let (client, handle) = mock_client();
        let mut cm = configmap("cm", "ns", &[]);
        cm.metadata.namespace = None;
        let err = add_finalizer_namespaced::<ConfigMap>(client, &cm, "op/f")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::KubeGenericError::MissingMetadata(_)
        ));
        assert_no_request(handle).await;
    }

    // -----------------------------------------------------------------------
    // remove_finalizer
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn remove_finalizer_is_noop_when_absent() {
        let (client, handle) = mock_client();
        let cm = configmap("cm", "ns", &["other/keep"]);
        remove_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
            .await
            .unwrap();
        assert_no_request(handle).await;
    }

    #[tokio::test]
    async fn remove_finalizer_removes_only_ours_by_index() {
        let (client, mut handle) = mock_client();
        let cm = configmap(
            "cm",
            "ns",
            &[
                "kubernetes.io/pvc-protection",
                "my-op/cleanup",
                "other/keep",
            ],
        );

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(content_type(&req), "application/json-patch+json");
            let body = read_body_json(req).await;
            assert_eq!(
                body,
                json!([
                    { "op": "test", "path": "/metadata/finalizers/1", "value": "my-op/cleanup" },
                    { "op": "remove", "path": "/metadata/finalizers/1" },
                ])
            );
            send.send_response(json_response(configmap_json(
                "cm",
                "ns",
                &["kubernetes.io/pvc-protection", "other/keep"],
            )));
        });

        let result =
            remove_finalizer::<ConfigMap, _>(client, Namespaced("ns"), &cm, "my-op/cleanup")
                .await
                .unwrap();
        assert_eq!(
            result.metadata.finalizers.unwrap(),
            vec![
                "kubernetes.io/pvc-protection".to_string(),
                "other/keep".to_string()
            ]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_finalizer_cluster_scoped_uri() {
        let (client, mut handle) = mock_client();
        let n = node("n1", &["my-op/cleanup"]);
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri().path().ends_with("/api/v1/nodes/n1"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(node_json("n1", &[])));
        });
        remove_finalizer::<Node, _>(client, Cluster, &n, "my-op/cleanup")
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn remove_finalizer_propagates_api_errors() {
        let (client, mut handle) = mock_client();
        let cm = configmap("cm", "ns", &["my-op/cleanup"]);
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
        });
        assert!(
            remove_finalizer_namespaced::<ConfigMap>(client, &cm, "my-op/cleanup")
                .await
                .is_err()
        );
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // remove_finalizers (deprecated) — still behaves as documented
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn deprecated_remove_finalizers_still_nulls_the_list() {
        let (client, mut handle) = mock_client();
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(content_type(&req), "application/merge-patch+json");
            let body = read_body_json(req).await;
            assert_eq!(body, json!({ "metadata": { "finalizers": null } }));
            send.send_response(json_response(node_json("n1", &[])));
        });
        remove_finalizers::<Node, _>(client, Cluster, "n1")
            .await
            .unwrap();
        server.await.unwrap();
    }
}
