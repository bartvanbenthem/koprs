// src/tests/owners.rs
//
// Unit tests for the `owners` module.
//
// Strategy
// --------
// Owner reference helpers (owner_ref, controller_ref, set_owner_refs) are pure
// functions — no HTTP call is made. Tests construct values directly from JSON.
//
// ObjectRef helpers (make_object_refs*) do make API calls, so those tests spin
// up a tower_test mock pair. make_object_ref_mapper is pure and tested without
// a mock.

#[cfg(test)]
mod owners_tests {
    use k8s_openapi::api::apps::v1::Deployment;
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::Resource;
    use serde_json::json;

    use crate::owners::{controller_ref, owner_ref, set_owner_refs};

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn configmap_with_uid(name: &str, namespace: &str, uid: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace, "uid": uid, "resourceVersion": "1" }
        }))
        .unwrap()
    }

    fn configmap_no_uid(name: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "resourceVersion": "1" }
        }))
        .unwrap()
    }

    fn configmap_no_name() -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "uid": "abc-123", "resourceVersion": "1" }
        }))
        .unwrap()
    }

    fn empty_deployment() -> Deployment {
        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "child", "namespace": "default", "resourceVersion": "1" }
        }))
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // owner_ref
    // -----------------------------------------------------------------------

    #[test]
    fn owner_ref_builds_correct_fields() {
        let cm = configmap_with_uid("my-cm", "default", "uid-abc");
        let oref = owner_ref(&cm).unwrap();

        assert_eq!(oref.name, "my-cm");
        assert_eq!(oref.uid, "uid-abc");
        assert_eq!(oref.kind, "ConfigMap");
        assert_eq!(oref.api_version, ConfigMap::api_version(&()).as_ref());
    }

    #[test]
    fn owner_ref_sets_controller_false() {
        let cm = configmap_with_uid("my-cm", "default", "uid-abc");
        let oref = owner_ref(&cm).unwrap();

        assert_eq!(oref.controller, Some(false));
        assert_eq!(oref.block_owner_deletion, Some(false));
    }

    #[test]
    fn owner_ref_errors_on_missing_uid() {
        let cm = configmap_no_uid("my-cm");
        let result = owner_ref(&cm);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("uid"), "expected 'uid' in error: {msg}");
    }

    #[test]
    fn owner_ref_errors_on_missing_name() {
        let cm = configmap_no_name();
        let result = owner_ref(&cm);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("name"), "expected 'name' in error: {msg}");
    }

    // -----------------------------------------------------------------------
    // controller_ref
    // -----------------------------------------------------------------------

    #[test]
    fn controller_ref_sets_controller_true() {
        let cm = configmap_with_uid("my-cm", "default", "uid-xyz");
        let oref = controller_ref(&cm).unwrap();

        assert_eq!(oref.controller, Some(true));
        assert_eq!(oref.block_owner_deletion, Some(true));
    }

    #[test]
    fn controller_ref_builds_correct_fields() {
        let cm = configmap_with_uid("my-cm", "default", "uid-xyz");
        let oref = controller_ref(&cm).unwrap();

        assert_eq!(oref.name, "my-cm");
        assert_eq!(oref.uid, "uid-xyz");
        assert_eq!(oref.kind, "ConfigMap");
    }

    // -----------------------------------------------------------------------
    // set_owner_refs
    // -----------------------------------------------------------------------

    #[test]
    fn set_owner_refs_writes_owner_references_to_child() {
        let cm = configmap_with_uid("parent", "default", "uid-parent");
        let oref = controller_ref(&cm).unwrap();

        let mut child = empty_deployment();
        set_owner_refs(&mut child, vec![oref.clone()]);

        let refs = child.meta().owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "parent");
        assert_eq!(refs[0].uid, "uid-parent");
        assert_eq!(refs[0].controller, Some(true));
    }

    #[test]
    fn set_owner_refs_replaces_existing_references() {
        let cm1 = configmap_with_uid("parent-1", "default", "uid-1");
        let cm2 = configmap_with_uid("parent-2", "default", "uid-2");

        let mut child = empty_deployment();
        set_owner_refs(&mut child, vec![owner_ref(&cm1).unwrap()]);
        set_owner_refs(&mut child, vec![controller_ref(&cm2).unwrap()]);

        let refs = child.meta().owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "parent-2");
    }

    #[test]
    fn set_owner_refs_accepts_multiple_owners() {
        let cm1 = configmap_with_uid("p1", "default", "uid-1");
        let cm2 = configmap_with_uid("p2", "default", "uid-2");

        let mut child = empty_deployment();
        set_owner_refs(
            &mut child,
            vec![owner_ref(&cm1).unwrap(), owner_ref(&cm2).unwrap()],
        );

        let refs = child.meta().owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 2);
    }

    // -----------------------------------------------------------------------
    // make_object_refs / make_object_refs_namespaced / make_object_refs_cluster
    // -----------------------------------------------------------------------

    use http::{Request, Response, StatusCode};
    use kube::client::Body;
    use tower_test::mock;

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (kube::Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (kube::Client::new(svc, "default"), handle)
    }

    fn json_response(body: serde_json::Value) -> Response<Body> {
        let bytes = serde_json::to_vec(&body).unwrap();
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(bytes))
            .unwrap()
    }

    fn configmap_list(names: &[&str], namespace: &str) -> serde_json::Value {
        let items: Vec<_> = names
            .iter()
            .map(|n| {
                json!({
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "metadata": { "name": n, "namespace": namespace, "resourceVersion": "1" }
                })
            })
            .collect();
        json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": { "resourceVersion": "1" },
            "items": items
        })
    }

    fn node_list(names: &[&str]) -> serde_json::Value {
        let items: Vec<_> = names
            .iter()
            .map(|n| {
                json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "metadata": { "name": n, "resourceVersion": "1" }
                })
            })
            .collect();
        json!({
            "apiVersion": "v1",
            "kind": "NodeList",
            "metadata": { "resourceVersion": "1" },
            "items": items
        })
    }

    use crate::owners::{make_object_ref_mapper, make_object_refs};
    use crate::scope::{Cluster, Namespaced};

    #[tokio::test]
    async fn make_object_refs_namespaced_returns_one_ref_per_resource() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/my-ns/configmaps")
            );
            send.send_response(json_response(configmap_list(&["cm1", "cm2"], "my-ns")));
        });

        let refs = make_object_refs::<ConfigMap, _>(client, Namespaced("my-ns"))
            .await
            .unwrap();
        assert_eq!(refs.len(), 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn make_object_refs_cluster_returns_one_ref_per_node() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(uri.contains("/api/v1/nodes"), "uri={uri}");
            assert!(!uri.contains("namespaces"), "uri={uri}");
            send.send_response(json_response(node_list(&["n1", "n2", "n3"])));
        });

        use k8s_openapi::api::core::v1::Node;
        let refs = make_object_refs::<Node, _>(client, Cluster).await.unwrap();
        assert_eq!(refs.len(), 3);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn make_object_refs_generic_namespaced_scopes_to_namespace() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert!(
                req.uri()
                    .to_string()
                    .contains("/namespaces/prod/configmaps")
            );
            send.send_response(json_response(configmap_list(&["cm-prod"], "prod")));
        });

        let refs = make_object_refs::<ConfigMap, _>(client, Namespaced("prod"))
            .await
            .unwrap();
        assert_eq!(refs.len(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn make_object_refs_returns_empty_vec_when_no_resources() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (_req, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(configmap_list(&[], "ns")));
        });

        let refs = make_object_refs::<ConfigMap, _>(client, Cluster)
            .await
            .unwrap();
        assert!(refs.is_empty());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // make_object_ref_mapper
    // -----------------------------------------------------------------------

    #[test]
    fn make_object_ref_mapper_returns_fixed_refs_for_any_trigger() {
        use k8s_openapi::api::core::v1::Node;
        use kube_runtime::reflector::ObjectRef;
        use std::sync::Arc;

        let ref1 = ObjectRef::<ConfigMap>::new("cm1");
        let ref2 = ObjectRef::<ConfigMap>::new("cm2");
        let refs = Arc::new(vec![ref1, ref2]);

        let mapper = make_object_ref_mapper::<Node, ConfigMap>(refs.clone());

        let node = serde_json::from_value::<Node>(json!({
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": { "name": "n1", "resourceVersion": "1" }
        }))
        .unwrap();

        let result = mapper(node);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn make_object_ref_mapper_returns_independent_clones() {
        use k8s_openapi::api::core::v1::Node;
        use kube_runtime::reflector::ObjectRef;
        use std::sync::Arc;

        let refs = Arc::new(vec![ObjectRef::<ConfigMap>::new("cm1")]);
        let mapper = make_object_ref_mapper::<Node, ConfigMap>(refs);

        let node1 = serde_json::from_value::<Node>(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": { "name": "n1", "resourceVersion": "1" }
        }))
        .unwrap();
        let node2 = serde_json::from_value::<Node>(json!({
            "apiVersion": "v1", "kind": "Node",
            "metadata": { "name": "n2", "resourceVersion": "1" }
        }))
        .unwrap();

        let r1 = mapper(node1);
        let r2 = mapper(node2);
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
    }
}
