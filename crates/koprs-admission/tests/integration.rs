// Integration tests for koprs-admission.
//
// These tests require the `integration` feature flag:
//
//   cargo test -p koprs-admission --features integration --test integration
//
// They spin up a real HTTP (plain, no TLS) webhook server on an OS-assigned
// port and drive it with actual HTTP requests.

#[cfg(feature = "integration")]
mod integration_tests {
    use std::time::Duration;

    use koprs_admission::webhook::{Validator, WebhookBuilder};
    use koprs_admission::{AdmissionRequest, ValidationResponse};
    use serde::{Deserialize, Serialize};
    use tokio::time::timeout;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct DummyResource {
        name: String,
    }

    struct AlwaysAllow;

    impl Validator<DummyResource> for AlwaysAllow {
        type Error = std::convert::Infallible;

        async fn validate(
            &self,
            _request: &AdmissionRequest<DummyResource>,
        ) -> Result<ValidationResponse, Self::Error> {
            Ok(ValidationResponse::allow())
        }
    }

    fn admission_review(name: &str) -> serde_json::Value {
        serde_json::json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "integ-uid-1",
                "name": name,
                "namespace": "default",
                "operation": "CREATE",
                "dryRun": false,
                "object": { "name": name },
            }
        })
    }

    /// Start a plain-HTTP webhook server on a random port and return the port.
    async fn start_server() -> u16 {
        // Bind port=0 to get an OS-assigned port, then pass it to the builder.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // release so WebhookBuilder can re-bind

        tokio::spawn(async move {
            WebhookBuilder::new()
                .port(port)
                // No TLS — plain HTTP for integration tests
                .validate("/validate/dummy", AlwaysAllow)
                .run()
                .await
                .ok();
        });

        // Give the server a moment to start listening
        tokio::time::sleep(Duration::from_millis(50)).await;
        port
    }

    #[tokio::test]
    async fn webhook_server_returns_allow_for_valid_resource() {
        let port = start_server().await;
        let client = reqwest::Client::new();

        let body = admission_review("my-resource");
        let resp = timeout(
            Duration::from_secs(5),
            client
                .post(format!("http://127.0.0.1:{port}/validate/dummy"))
                .json(&body)
                .send(),
        )
        .await
        .expect("timed out")
        .expect("request failed");

        assert_eq!(resp.status(), 200);
        let v: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(v["response"]["allowed"], true);
        assert_eq!(v["response"]["uid"], "integ-uid-1");
    }

    #[tokio::test]
    async fn webhook_server_returns_404_for_unregistered_path() {
        let port = start_server().await;
        let client = reqwest::Client::new();

        let body = admission_review("x");
        let resp = timeout(
            Duration::from_secs(5),
            client
                .post(format!("http://127.0.0.1:{port}/validate/unknown"))
                .json(&body)
                .send(),
        )
        .await
        .expect("timed out")
        .expect("request failed");

        assert_eq!(resp.status(), 404);
    }
}
