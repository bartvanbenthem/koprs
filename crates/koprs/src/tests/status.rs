// src/tests/status.rs

#[cfg(test)]
mod status_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::Client;
    use kube::client::Body;
    use serde::Serialize;
    use serde_json::json;
    use tower_test::mock;

    use crate::scope::{Cluster, Namespaced};
    use crate::status::{
        make_condition, patch_status, patch_status_cluster, patch_status_namespaced,
        upsert_condition,
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

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // Fixture builders
    // -----------------------------------------------------------------------

    fn configmap_json(name: &str, namespace: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace, "resourceVersion": "1" }
        })
    }

    fn node_json(name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": name, "resourceVersion": "1" }
        })
    }

    // -----------------------------------------------------------------------
    // Status types used in tests
    // -----------------------------------------------------------------------

    /// Minimal status struct — anything Serialize-able is valid.
    #[derive(Serialize)]
    struct SimpleStatus {
        ready: bool,
    }

    /// A richer status with multiple fields to verify they all appear in the
    /// patch body.
    #[derive(Serialize)]
    struct RichStatus {
        ready: bool,
        message: String,
        observed_generation: i64,
    }

    // -----------------------------------------------------------------------
    // patch_status — URI must point at the /status subresource
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_namespaced_uri_contains_status_subresource() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            // kube appends /status to the resource path for patch_status calls
            assert!(
                uri.contains("/namespaces/my-ns/configmaps/my-cm/status"),
                "expected /status subresource in uri, got: {uri}"
            );
            send.send_response(json_response(configmap_json("my-cm", "my-ns")));
        });

        patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("my-ns"),
            "my-cm",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_cluster_uri_contains_status_subresource_without_namespace() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), http::Method::PATCH);
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/api/v1/nodes/my-node/status"),
                "expected /status subresource in uri, got: {uri}"
            );
            assert!(
                !uri.contains("namespaces"),
                "cluster-scoped resource must not have a namespace segment, got: {uri}"
            );
            send.send_response(json_response(node_json("my-node")));
        });

        patch_status::<Node, _, _>(
            client,
            Cluster,
            "my-node",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // patch_status — SSA query params
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_sends_ssa_field_manager_and_force_params() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            // PatchParams::apply(fm).force() produces ?fieldManager=…&force=true
            assert!(
                uri.contains("fieldManager=my-op"),
                "expected fieldManager param in uri, got: {uri}"
            );
            assert!(
                uri.contains("force=true"),
                "expected force=true param in uri, got: {uri}"
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // patch_status — patch body structure
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_body_contains_api_version_and_kind() {
        // apply_status_patch builds the body from K::api_version and K::kind.
        // These must be present for SSA to work — without them the API server
        // cannot identify the resource type and will reject the request.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(
                body["apiVersion"], "v1",
                "patch body must include apiVersion"
            );
            assert_eq!(body["kind"], "ConfigMap", "patch body must include kind");
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_body_contains_status_field() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(
                body["status"]["ready"], true,
                "patch body must nest status under the 'status' key"
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_body_contains_all_status_fields() {
        // Verifies that the entire status struct is serialised, not just the
        // first field or a partial view.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert_eq!(body["status"]["ready"], true);
            assert_eq!(body["status"]["message"], "all good");
            assert_eq!(body["status"]["observed_generation"], 42);
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            RichStatus {
                ready: true,
                message: "all good".to_string(),
                observed_generation: 42,
            },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_body_does_not_contain_spec_or_metadata_fields() {
        // The patch must only carry apiVersion, kind, and status.
        // Leaking spec or metadata into a status SSA patch can cause
        // unintended field ownership conflicts.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let body = read_body_json(req).await;
            assert!(
                body.get("spec").is_none(),
                "patch body must not contain spec"
            );
            assert!(
                body.get("metadata").is_none(),
                "patch body must not contain metadata"
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            SimpleStatus { ready: false },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // patch_status — return value
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_returns_deserialised_resource_from_server_response() {
        // The function returns whatever the server sends back, not the patch
        // we sent. This verifies that the response path is wired correctly.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        let result = patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        assert_eq!(result.metadata.name.as_deref(), Some("cm1"));
        assert_eq!(result.metadata.namespace.as_deref(), Some("ns1"));

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // patch_status_namespaced — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_namespaced_wrapper_routes_to_correct_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/ns1/configmaps/cm1/status"),
                "uri={uri}"
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status_namespaced::<ConfigMap, _>(
            client,
            "ns1",
            "cm1",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_namespaced_wrapper_forwards_field_manager() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri().to_string().contains("fieldManager=specific-op"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(configmap_json("cm1", "ns1")));
        });

        patch_status_namespaced::<ConfigMap, _>(
            client,
            "ns1",
            "cm1",
            SimpleStatus { ready: true },
            "specific-op",
        )
        .await
        .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // patch_status_cluster — convenience wrapper
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_cluster_wrapper_routes_to_correct_uri() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes/n1/status"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_json("n1")));
        });

        patch_status_cluster::<Node, _>(client, "n1", SimpleStatus { ready: true }, "my-op")
            .await
            .unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_cluster_wrapper_forwards_field_manager() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri().to_string().contains("fieldManager=cluster-op"),
                "uri={}",
                req.uri()
            );
            send.send_response(json_response(node_json("n1")));
        });

        patch_status_cluster::<Node, _>(client, "n1", SimpleStatus { ready: true }, "cluster-op")
            .await
            .unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // Error propagation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn patch_status_propagates_server_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result = patch_status::<ConfigMap, _, _>(
            client,
            Namespaced("ns1"),
            "cm1",
            SimpleStatus { ready: true },
            "my-op",
        )
        .await;

        assert!(result.is_err(), "expected Err on 500, got Ok");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn patch_status_cluster_propagates_server_errors() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(server_error_response());
        });

        let result =
            patch_status_cluster::<Node, _>(client, "n1", SimpleStatus { ready: true }, "my-op")
                .await;

        assert!(result.is_err(), "expected Err on 500, got Ok");
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // make_condition — pure
    // -----------------------------------------------------------------------

    #[test]
    fn make_condition_sets_all_fields() {
        let c = make_condition("Ready", "True", "Reconciled", "All good", Some(3));
        assert_eq!(c.type_, "Ready");
        assert_eq!(c.status, "True");
        assert_eq!(c.reason, "Reconciled");
        assert_eq!(c.message, "All good");
        assert_eq!(c.observed_generation, Some(3));
    }

    #[test]
    fn make_condition_sets_last_transition_time() {
        let before = chrono::Utc::now();
        let c = make_condition("Ready", "True", "R", "M", None);
        let after = chrono::Utc::now();
        assert!(c.last_transition_time.0 >= before);
        assert!(c.last_transition_time.0 <= after);
    }

    // -----------------------------------------------------------------------
    // upsert_condition — pure
    // -----------------------------------------------------------------------

    #[test]
    fn upsert_condition_appends_when_type_is_new() {
        let mut conditions = vec![make_condition("Ready", "True", "R", "M", None)];
        upsert_condition(
            &mut conditions,
            make_condition("Synced", "False", "R", "M", None),
        );
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[1].type_, "Synced");
    }

    #[test]
    fn upsert_condition_updates_existing_by_type() {
        let mut conditions = vec![make_condition("Ready", "False", "Init", "Not ready", None)];
        upsert_condition(
            &mut conditions,
            make_condition("Ready", "True", "Done", "Ready", None),
        );
        assert_eq!(conditions.len(), 1);
        assert_eq!(conditions[0].status, "True");
        assert_eq!(conditions[0].reason, "Done");
    }

    #[test]
    fn upsert_condition_preserves_transition_time_when_status_unchanged() {
        let original = make_condition("Ready", "True", "R1", "M1", None);
        let original_time = original.last_transition_time.clone();
        let mut conditions = vec![original];

        // Same status — should not update lastTransitionTime
        let updated = make_condition("Ready", "True", "R2", "M2 updated", None);
        upsert_condition(&mut conditions, updated);

        assert_eq!(conditions[0].last_transition_time, original_time);
        assert_eq!(conditions[0].message, "M2 updated");
    }

    #[test]
    fn upsert_condition_updates_transition_time_when_status_changes() {
        let original = make_condition("Ready", "False", "Init", "Not ready", None);
        let original_time = original.last_transition_time.clone();
        let mut conditions = vec![original];

        // Status changed — lastTransitionTime must be updated
        let updated = make_condition("Ready", "True", "Done", "Ready now", None);
        let new_time = updated.last_transition_time.clone();
        upsert_condition(&mut conditions, updated);

        assert_ne!(conditions[0].last_transition_time, original_time);
        assert_eq!(conditions[0].last_transition_time, new_time);
    }
}
