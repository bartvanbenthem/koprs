// src/tests/scope.rs
//
// Testing strategy
// ----------------
// Cluster and Namespaced are compile-time scope markers. Their only runtime
// behaviour is:
//
//   - namespace() → Option<&str>   (observable, assert on the value)
//   - into_api()  → Api<K>         (not directly inspectable, but the URI
//                                   it produces is — verified via the mock
//                                   client in the "API construction" section)
//
// ApiScope is sealed, so we cannot add new impls outside the crate. The tests
// here confirm:
//   1. Cluster and Namespaced implement ApiScope for the expected resource types.
//   2. namespace() returns the correct value for each marker.
//   3. into_api() routes to Api::all (no namespace segment) for Cluster and
//      Api::namespaced (with namespace segment) for Namespaced.
//   4. Both markers are Copy + Clone (structural, verified by the compiler).
//
// Two quirks of the ApiScope trait drive most of the syntax choices:
//
//   a) namespace() is generic over K (lives on ApiScope<K>), so the compiler
//      cannot infer K when calling it on a bare marker value. We use the
//      fully-qualified path ApiScope::<K>::namespace(&marker) to disambiguate.
//
//   b) into_api() has no generic parameters of its own — K is on the trait.
//      The turbofish .into_api::<K>() is therefore illegal; the correct form
//      is ApiScope::<K>::into_api(marker, client).

#[cfg(test)]
mod scope_tests {
    use http::{Request, Response, StatusCode};
    use k8s_openapi::api::core::v1::{ConfigMap, Node};
    use kube::client::Body;
    use kube::Client;
    use serde_json::json;
    use tower_test::mock;

    use crate::scope::{ApiScope, Cluster, Namespaced};

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

    // -----------------------------------------------------------------------
    // namespace() — the only runtime-observable field on the markers
    //
    // namespace() is a method on ApiScope<K>. Calling it as marker.namespace()
    // leaves K ambiguous, so we use the fully-qualified form throughout.
    // Node is used as the concrete K for Cluster tests; ConfigMap for Namespaced.
    // The choice of K does not affect namespace() — it returns a fixed value
    // regardless of the resource type.
    // -----------------------------------------------------------------------

    #[test]
    fn cluster_namespace_returns_none() {
        assert_eq!(ApiScope::<Node>::namespace(&Cluster), None);
    }

    #[test]
    fn namespaced_namespace_returns_the_inner_str() {
        assert_eq!(
            ApiScope::<ConfigMap>::namespace(&Namespaced("my-ns")),
            Some("my-ns")
        );
    }

    #[test]
    fn namespaced_namespace_returns_exact_str_including_edge_cases() {
        assert_eq!(ApiScope::<ConfigMap>::namespace(&Namespaced("")), Some(""));
        assert_eq!(
            ApiScope::<ConfigMap>::namespace(&Namespaced("kube-system")),
            Some("kube-system")
        );
        assert_eq!(
            ApiScope::<ConfigMap>::namespace(&Namespaced("a-b-c-123")),
            Some("a-b-c-123")
        );
    }

    // -----------------------------------------------------------------------
    // Copy + Clone — structural, verified by the compiler
    // -----------------------------------------------------------------------

    #[test]
    fn cluster_is_copy() {
        let a = Cluster;
        let b = a; // copy
        let _ = a; // original still usable — proves Copy, not just Clone
        let _ = b;
    }

    #[test]
    fn cluster_is_clone() {
        let a = Cluster;
        let b = a.clone();
        let _ = b;
    }

    #[test]
    fn namespaced_is_copy() {
        let a = Namespaced("ns");
        let b = a; // copy
        let _ = a; // original still usable
        let _ = b;
    }

    #[test]
    fn namespaced_is_clone() {
        let a = Namespaced("ns");
        let b = a.clone();
        // Use fully-qualified form to disambiguate K.
        assert_eq!(ApiScope::<ConfigMap>::namespace(&b), Some("ns"));
    }

    // -----------------------------------------------------------------------
    // into_api — Cluster resolves to Api::all (no namespace in URI)
    //
    // K is on the trait, not on the method, so we use:
    //   ApiScope::<K>::into_api(marker, client)
    // rather than the illegal marker.into_api::<K>(client).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn cluster_into_api_produces_all_api_without_namespace_segment() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/api/v1/nodes"),
                "expected cluster-scoped nodes URI, got: {uri}"
            );
            assert!(
                !uri.contains("namespaces"),
                "Api::all must not contain a namespace segment, got: {uri}"
            );
            send.send_response(ok_list_response("NodeList"));
        });

        let api = ApiScope::<Node>::into_api(Cluster, client);
        api.list(&Default::default()).await.unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // into_api — Namespaced resolves to Api::namespaced (namespace in URI)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn namespaced_into_api_produces_namespaced_api_with_correct_namespace() {
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/prod/configmaps"),
                "expected namespace-scoped configmaps URI, got: {uri}"
            );
            send.send_response(ok_list_response("ConfigMapList"));
        });

        let api = ApiScope::<ConfigMap>::into_api(Namespaced("prod"), client);
        api.list(&Default::default()).await.unwrap();

        server.await.unwrap();
    }

    #[tokio::test]
    async fn namespaced_into_api_uses_exact_namespace_string() {
        // Verifies the namespace value passed to Namespaced is forwarded
        // verbatim to the URI — no trimming, lowercasing, or transformation.
        let (client, mut handle) = mock_client();

        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            let uri = req.uri().to_string();
            assert!(
                uri.contains("/namespaces/kube-system/configmaps"),
                "uri={uri}"
            );
            send.send_response(ok_list_response("ConfigMapList"));
        });

        let api = ApiScope::<ConfigMap>::into_api(Namespaced("kube-system"), client);
        api.list(&Default::default()).await.unwrap();

        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // ApiScope is implemented for the expected resource / scope combinations
    // -----------------------------------------------------------------------
    //
    // Compile-time bound witnesses. If a combination fails to compile, the
    // impl is missing or incorrectly bounded.

    fn assert_api_scope<K, S: ApiScope<K>>()
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
    fn cluster_implements_api_scope_for_namespaced_resources() {
        // Cluster wraps any resource via Api::all — useful for cross-namespace
        // list operations.
        assert_api_scope::<ConfigMap, Cluster>();
    }

    #[test]
    fn namespaced_implements_api_scope_for_namespaced_resources() {
        assert_api_scope::<ConfigMap, Namespaced<'_>>();
    }

    // -----------------------------------------------------------------------
    // Sealed trait — ApiScope cannot be implemented outside the crate.
    //
    // Enforced structurally: private::Sealed is not re-exported, so no
    // external type can satisfy the supertrait bound. The negative case
    // belongs in a compile_fail doc-test on ApiScope rather than here.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // namespace() is consistent before and after clone
    // -----------------------------------------------------------------------

    #[test]
    fn namespaced_clone_preserves_namespace_value() {
        let original = Namespaced("staging");
        let cloned = original.clone();
        assert_eq!(
            ApiScope::<ConfigMap>::namespace(&original),
            ApiScope::<ConfigMap>::namespace(&cloned),
        );
    }

    #[test]
    fn cluster_clone_namespace_is_still_none() {
        let original = Cluster;
        let cloned = original.clone();
        assert_eq!(
            ApiScope::<Node>::namespace(&original),
            ApiScope::<Node>::namespace(&cloned),
        );
    }
}