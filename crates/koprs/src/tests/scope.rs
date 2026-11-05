// src/tests/scope.rs
//
// Testing strategy
// ----------------
// Cluster, Namespaced and AllNamespaces are compile-time scope markers. Their
// runtime behaviour is:
//
//   - namespace()   → Option<&str>   (assert on the value)
//   - into_api()    → Api<K>         (verified via the URI it produces)
//   - object_api()  → Api<K>         (verified via the URI it produces)
//
// The compile-time guarantees (Cluster rejected for namespaced types,
// AllNamespaces rejected for single-object operations) are covered by
// `compile_fail` doc-tests in `scope.rs`.
//
// The methods are generic over K via the trait, so the fully-qualified form
// `ListScope::<K>::method(...)` is used to pick K.

#[cfg(test)]
mod scope_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::scope::{AllNamespaces, ApiScope, Cluster, ListScope, Namespaced};

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (Client::new(svc, "default"), handle)
    }

    fn ok_list_response(kind: &str) -> Response<Body> {
        let body = json!({
            "apiVersion": "v1",
            "kind": kind,
            "metadata": {},
            "items": []
        });
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    /// Issue one list through `api` and return the request URI.
    async fn list_uri<K>(api: kube::Api<K>, mut handle: MockHandle, kind: &'static str) -> String
    where
        K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
    {
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            send.send_response(ok_list_response(kind));
            uri
        });
        api.list(&Default::default()).await.unwrap();
        server.await.unwrap()
    }

    // -----------------------------------------------------------------------
    // namespace()
    // -----------------------------------------------------------------------

    #[test]
    fn cluster_namespace_returns_none() {
        assert_eq!(ListScope::<Node>::namespace(&Cluster), None);
    }

    #[test]
    fn all_namespaces_namespace_returns_none() {
        assert_eq!(ListScope::<ConfigMap>::namespace(&AllNamespaces), None);
    }

    #[test]
    fn namespaced_namespace_returns_the_inner_str() {
        assert_eq!(
            ListScope::<ConfigMap>::namespace(&Namespaced("my-ns")),
            Some("my-ns")
        );
    }

    #[test]
    fn namespaced_namespace_returns_exact_str_including_edge_cases() {
        assert_eq!(ListScope::<ConfigMap>::namespace(&Namespaced("")), Some(""));
        assert_eq!(
            ListScope::<ConfigMap>::namespace(&Namespaced("kube-system")),
            Some("kube-system")
        );
    }

    // -----------------------------------------------------------------------
    // Copy — structural, verified by the compiler
    // -----------------------------------------------------------------------

    #[test]
    fn markers_are_copy() {
        fn assert_copy<T: Copy>(_: T) {}
        assert_copy(Cluster);
        assert_copy(Namespaced("ns"));
        assert_copy(AllNamespaces);
    }

    // -----------------------------------------------------------------------
    // into_api — URIs
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cluster_into_api_produces_all_api_without_namespace_segment() {
        let (client, handle) = mock_client();
        let api = ListScope::<Node>::into_api(Cluster, client);
        let uri = list_uri(api, handle, "NodeList").await;
        assert!(uri.contains("/api/v1/nodes"), "uri={uri}");
        assert!(!uri.contains("namespaces"), "uri={uri}");
    }

    #[tokio::test]
    async fn namespaced_into_api_produces_namespaced_api_with_correct_namespace() {
        let (client, handle) = mock_client();
        let api = ListScope::<ConfigMap>::into_api(Namespaced("prod"), client);
        let uri = list_uri(api, handle, "ConfigMapList").await;
        assert!(uri.contains("/namespaces/prod/configmaps"), "uri={uri}");
    }

    #[tokio::test]
    async fn all_namespaces_into_api_lists_across_namespaces() {
        let (client, handle) = mock_client();
        let api = ListScope::<ConfigMap>::into_api(AllNamespaces, client);
        let uri = list_uri(api, handle, "ConfigMapList").await;
        assert!(uri.contains("/api/v1/configmaps"), "uri={uri}");
        assert!(!uri.contains("namespaces"), "uri={uri}");
    }

    // -----------------------------------------------------------------------
    // object_api — writes target the object's own namespace
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn all_namespaces_object_api_uses_the_objects_namespace() {
        let (client, handle) = mock_client();
        let api = ListScope::<ConfigMap>::object_api(&AllNamespaces, client, Some("team-b"));
        let uri = list_uri(api, handle, "ConfigMapList").await;
        assert!(uri.contains("/namespaces/team-b/configmaps"), "uri={uri}");
    }

    #[tokio::test]
    async fn namespaced_object_api_uses_the_scope_namespace() {
        let (client, handle) = mock_client();
        let api = ListScope::<ConfigMap>::object_api(&Namespaced("team-a"), client, Some("team-a"));
        let uri = list_uri(api, handle, "ConfigMapList").await;
        assert!(uri.contains("/namespaces/team-a/configmaps"), "uri={uri}");
    }

    #[tokio::test]
    async fn cluster_object_api_has_no_namespace_segment() {
        let (client, handle) = mock_client();
        let api = ListScope::<Node>::object_api(&Cluster, client, None);
        let uri = list_uri(api, handle, "NodeList").await;
        assert!(!uri.contains("namespaces"), "uri={uri}");
    }

    // -----------------------------------------------------------------------
    // Trait implementation witnesses
    // -----------------------------------------------------------------------

    fn assert_api_scope<K, S: ApiScope<K>>()
    where
        K: kube::Resource<DynamicType = ()>
            + Clone
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
    }

    fn assert_list_scope<K, S: ListScope<K>>()
    where
        K: kube::Resource<DynamicType = ()>
            + Clone
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
    }

    #[test]
    fn cluster_implements_api_scope_for_cluster_scoped_resources() {
        assert_api_scope::<Node, Cluster>();
    }

    #[test]
    fn namespaced_implements_api_scope_for_namespaced_resources() {
        assert_api_scope::<ConfigMap, Namespaced<'_>>();
    }

    #[test]
    fn all_namespaces_implements_list_scope_for_namespaced_resources() {
        assert_list_scope::<ConfigMap, AllNamespaces>();
    }
}
