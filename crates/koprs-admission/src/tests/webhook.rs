// src/tests/webhook.rs
//
// Testing strategy
// ----------------
// Validator logic is tested by calling admit() directly with raw JSON bodies.
// WebhookBuilder configuration is tested by inspecting field values after
// each builder call. Full HTTP round-trips are exercised using
// tower::ServiceExt::oneshot on the axum router — no real TCP listener or
// TLS required. The health server is tested via a real TCP connection on an
// OS-assigned port.

#[cfg(test)]
mod webhook_tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use http::Request;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tower::ServiceExt;

    use crate::review::{AdmissionRequest, ValidationResponse};
    use crate::webhook::{Validator, WebhookBuilder, serve_health};

    // -----------------------------------------------------------------------
    // Test resource and validators
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestResource {
        name: String,
        replicas: u32,
    }

    struct AlwaysAllow;

    impl Validator<TestResource> for AlwaysAllow {
        type Error = std::convert::Infallible;

        fn validate(
            &self,
            _request: &AdmissionRequest<TestResource>,
        ) -> impl Future<Output = Result<ValidationResponse, Self::Error>> + Send {
            async { Ok(ValidationResponse::allow()) }
        }
    }

    struct AlwaysDeny;

    impl Validator<TestResource> for AlwaysDeny {
        type Error = std::convert::Infallible;

        fn validate(
            &self,
            _request: &AdmissionRequest<TestResource>,
        ) -> impl Future<Output = Result<ValidationResponse, Self::Error>> + Send {
            async { Ok(ValidationResponse::deny("always denied")) }
        }
    }

    struct ReplicaLimit {
        max: u32,
    }

    impl Validator<TestResource> for ReplicaLimit {
        type Error = std::convert::Infallible;

        fn validate(
            &self,
            request: &AdmissionRequest<TestResource>,
        ) -> impl Future<Output = Result<ValidationResponse, Self::Error>> + Send {
            let max = self.max;
            let replicas = request.object.as_ref().map(|r| r.replicas);
            async move {
                match replicas {
                    Some(n) if n > max => Ok(ValidationResponse::deny(format!(
                        "replicas {n} exceeds limit {max}"
                    ))),
                    _ => Ok(ValidationResponse::allow()),
                }
            }
        }
    }

    struct ErrorValidator;

    #[derive(Debug)]
    struct ValidatorError;

    impl std::fmt::Display for ValidatorError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "internal validator error")
        }
    }

    impl std::error::Error for ValidatorError {}

    impl Validator<TestResource> for ErrorValidator {
        type Error = ValidatorError;

        fn validate(
            &self,
            _request: &AdmissionRequest<TestResource>,
        ) -> impl Future<Output = Result<ValidationResponse, Self::Error>> + Send {
            async { Err(ValidatorError) }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_review(operation: &str, replicas: u32, name: &str) -> serde_json::Value {
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "test-uid",
                "name": name,
                "namespace": "default",
                "operation": operation,
                "dryRun": false,
                "object": { "replicas": replicas, "name": name },
            }
        })
    }

    fn make_router(path: &str, validator: impl Validator<TestResource>) -> axum::Router {
        let mut router = axum::Router::new();
        let v = Arc::new(validator);
        router = router.route(
            path,
            axum::routing::post(move |axum::Json(body): axum::Json<serde_json::Value>| {
                let validator = v.clone();
                async move {
                    use crate::webhook::admit_test;
                    admit_test::<TestResource, _>(&validator, body).await
                }
            }),
        );
        router
    }

    fn post_json(path: &str, body: serde_json::Value) -> Request<axum::body::Body> {
        Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    async fn response_json(
        router: axum::Router,
        req: Request<axum::body::Body>,
    ) -> serde_json::Value {
        use http_body_util::BodyExt;
        let resp = router.oneshot(req).await.unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // WebhookBuilder — construction and field values
    // -----------------------------------------------------------------------

    #[test]
    fn builder_default_port_is_8443() {
        let b = WebhookBuilder::new();
        assert_eq!(b.port, 8443);
    }

    #[test]
    fn builder_port_sets_port() {
        let b = WebhookBuilder::new().port(9443);
        assert_eq!(b.port, 9443);
    }

    #[test]
    fn builder_health_port_defaults_to_none() {
        let b = WebhookBuilder::new();
        assert_eq!(b.health_port, None);
    }

    #[test]
    fn builder_health_port_sets_port() {
        let b = WebhookBuilder::new().health_port(8080);
        assert_eq!(b.health_port, Some(8080));
    }

    #[test]
    fn builder_graceful_shutdown_defaults_false() {
        let b = WebhookBuilder::new();
        assert!(!b.graceful_shutdown);
    }

    #[test]
    fn builder_graceful_shutdown_sets_flag() {
        let b = WebhookBuilder::new().graceful_shutdown();
        assert!(b.graceful_shutdown);
    }

    #[test]
    fn builder_tls_defaults_to_none() {
        let b = WebhookBuilder::new();
        assert!(b.tls.is_none());
    }

    #[test]
    fn builder_validate_adds_route() {
        let b = WebhookBuilder::new().validate("/validate/test", AlwaysAllow);
        assert_eq!(b.routes.len(), 1);
        assert_eq!(b.routes[0].0, "/validate/test");
    }

    #[test]
    fn builder_validate_multiple_routes_compose() {
        let b = WebhookBuilder::new()
            .validate("/validate/a", AlwaysAllow)
            .validate("/validate/b", AlwaysDeny);
        assert_eq!(b.routes.len(), 2);
    }

    #[test]
    fn builder_full_chain_does_not_panic() {
        let _b = WebhookBuilder::new()
            .port(9443)
            .health_port(8080)
            .graceful_shutdown()
            .validate("/validate/test", AlwaysAllow);
    }

    // -----------------------------------------------------------------------
    // admit — AlwaysAllow path via HTTP round-trip
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admit_always_allow_returns_allowed_true() {
        let router = make_router("/validate/test", AlwaysAllow);
        let body = make_review("CREATE", 3, "my-app");
        let v = response_json(router, post_json("/validate/test", body)).await;
        assert_eq!(v["response"]["allowed"], true);
        assert_eq!(v["response"]["uid"], "test-uid");
    }

    // -----------------------------------------------------------------------
    // admit — AlwaysDeny path
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admit_always_deny_returns_allowed_false() {
        let router = make_router("/validate/test", AlwaysDeny);
        let body = make_review("CREATE", 1, "my-app");
        let v = response_json(router, post_json("/validate/test", body)).await;
        assert_eq!(v["response"]["allowed"], false);
        assert_eq!(v["response"]["status"]["message"], "always denied");
    }

    // -----------------------------------------------------------------------
    // admit — ReplicaLimit enforces threshold
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admit_replica_limit_allows_under_limit() {
        let router = make_router("/validate/test", ReplicaLimit { max: 5 });
        let body = make_review("CREATE", 3, "app");
        let v = response_json(router, post_json("/validate/test", body)).await;
        assert_eq!(v["response"]["allowed"], true);
    }

    #[tokio::test]
    async fn admit_replica_limit_denies_over_limit() {
        let router = make_router("/validate/test", ReplicaLimit { max: 5 });
        let body = make_review("CREATE", 10, "app");
        let v = response_json(router, post_json("/validate/test", body)).await;
        assert_eq!(v["response"]["allowed"], false);
        let msg = v["response"]["status"]["message"].as_str().unwrap();
        assert!(
            msg.contains("10"),
            "message should mention the replica count"
        );
        assert!(msg.contains("5"), "message should mention the limit");
    }

    // -----------------------------------------------------------------------
    // admit — validator error returns 500
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admit_validator_error_returns_500() {
        // ErrorValidator returns Err — the server should respond 500
        let v = Arc::new(ErrorValidator);
        let handler = move |axum::Json(body): axum::Json<serde_json::Value>| {
            let validator = v.clone();
            async move {
                use crate::webhook::admit_test;
                admit_test::<TestResource, _>(&validator, body).await
            }
        };
        let router = axum::Router::new().route("/validate/test", axum::routing::post(handler));
        let body = make_review("CREATE", 1, "app");
        let req = post_json("/validate/test", body);
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::INTERNAL_SERVER_ERROR);
    }

    // -----------------------------------------------------------------------
    // admit — malformed body is denied gracefully
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admit_missing_request_field_returns_deny() {
        // Body is valid JSON but missing the "request" field
        let router = make_router("/validate/test", AlwaysAllow);
        let body = json!({ "apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview" });
        let v = response_json(router, post_json("/validate/test", body)).await;
        assert_eq!(v["response"]["allowed"], false);
    }

    // -----------------------------------------------------------------------
    // admit — uid is echoed back in response
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn admit_response_echoes_request_uid() {
        let router = make_router("/validate/test", AlwaysAllow);
        let mut body = make_review("CREATE", 1, "app");
        body["request"]["uid"] = json!("my-unique-uid");
        let v = response_json(router, post_json("/validate/test", body)).await;
        assert_eq!(v["response"]["uid"], "my-unique-uid");
    }

    // -----------------------------------------------------------------------
    // serve_health — real TCP, tests all branches
    // -----------------------------------------------------------------------

    async fn start_health_server() -> (u16, Arc<AtomicBool>) {
        let ready = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve_health(listener, ready.clone()));
        (port, ready)
    }

    async fn http_get(port: u16, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 512];
        let n = stream.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    #[tokio::test]
    async fn health_server_healthz_returns_200() {
        let (port, _) = start_health_server().await;
        let resp = http_get(port, "/healthz").await;
        assert!(resp.starts_with("HTTP/1.1 200"), "got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_returns_503_when_not_ready() {
        let (port, _) = start_health_server().await;
        let resp = http_get(port, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 503"), "got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_returns_200_when_ready() {
        let (port, ready) = start_health_server().await;
        ready.store(true, Ordering::Release);
        // Give the server a tick to pick up the flag
        tokio::time::sleep(Duration::from_millis(10)).await;
        let resp = http_get(port, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 200"), "got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_transitions_from_503_to_200() {
        let (port, ready) = start_health_server().await;
        let before = http_get(port, "/readyz").await;
        assert!(before.starts_with("HTTP/1.1 503"), "expected 503: {before}");
        ready.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let after = http_get(port, "/readyz").await;
        assert!(after.starts_with("HTTP/1.1 200"), "expected 200: {after}");
    }
}
