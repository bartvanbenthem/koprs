// src/tests/http.rs
//
// Testing strategy
// ----------------
// Each test binds an ephemeral axum server (port 0, OS-assigned) with
// shared state, exercises one behaviour of HttpPoller against it, and
// shuts down by simply dropping the server handle. tokio::time::timeout
// guards every poll call so a broken poller cannot hang the suite.

#[cfg(test)]
mod http_tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, HeaderValue, StatusCode},
        response::IntoResponse,
        routing::get,
    };
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    use crate::http::HttpPoller;
    use crate::watcher::{ExternalEvent, ExternalSource};

    // -----------------------------------------------------------------------
    // Mock server infrastructure
    // -----------------------------------------------------------------------

    /// Shared state for the mock server. Both the test and the handler hold
    /// a clone of this struct (Arc inside).
    #[derive(Clone, Default)]
    struct MockState {
        body: Arc<Mutex<Option<String>>>,
        etag: Arc<Mutex<Option<String>>>,
    }

    async fn resource_handler(
        State(state): State<MockState>,
        headers: HeaderMap,
    ) -> impl IntoResponse {
        let body = state.body.lock().unwrap().clone();
        let etag_val = state.etag.lock().unwrap().clone();

        let Some(body_text) = body else {
            return StatusCode::NOT_FOUND.into_response();
        };

        // Conditional GET: return 304 if the client's If-None-Match matches
        if let Some(ref etag) = etag_val {
            if let Some(inm) = headers.get("if-none-match") {
                if inm.to_str().unwrap_or("") == etag.as_str() {
                    return StatusCode::NOT_MODIFIED.into_response();
                }
            }
        }

        let mut resp_headers = HeaderMap::new();
        if let Some(ref etag) = etag_val {
            resp_headers.insert("etag", HeaderValue::from_str(etag).unwrap());
        }
        (StatusCode::OK, resp_headers, body_text).into_response()
    }

    async fn start_server(state: MockState) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let router = Router::new()
            .route("/resource", get(resource_handler))
            .with_state(state);
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        format!("http://{addr}/resource")
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    async fn poll_with_timeout(
        poller: &mut HttpPoller,
    ) -> Vec<ExternalEvent<crate::http::HttpResponse>> {
        timeout(Duration::from_secs(2), poller.poll())
            .await
            .expect("poll timed out")
            .expect("poll returned an error")
    }

    // -----------------------------------------------------------------------
    // HttpPoller::new — default name is the URL
    // -----------------------------------------------------------------------

    #[test]
    fn http_poller_name_defaults_to_url() {
        let poller = HttpPoller::new("http://localhost:9999/api");
        assert_eq!(poller.name(), "http://localhost:9999/api");
    }

    #[test]
    fn http_poller_with_name_overrides_default() {
        let poller = HttpPoller::new("http://localhost:9999/api").with_name("my-api");
        assert_eq!(poller.name(), "my-api");
    }

    // -----------------------------------------------------------------------
    // poll — Added on first 200
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_added_on_first_successful_response() {
        let state = MockState::default();
        *state.body.lock().unwrap() = Some("hello world".to_string());

        let url = start_server(state).await;
        let mut poller = HttpPoller::new(url);

        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1, "expected exactly one event");
        assert!(
            matches!(events[0], ExternalEvent::Added(_)),
            "expected Added on first 200, got {:?}",
            events[0]
        );
        if let ExternalEvent::Added(ref r) = events[0] {
            assert_eq!(r.status, 200);
            assert_eq!(r.body.as_ref(), b"hello world");
        }
    }

    // -----------------------------------------------------------------------
    // poll — no events when resource does not exist and was never seen
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_nothing_on_404_when_resource_was_never_seen() {
        let state = MockState::default(); // body is None → server returns 404

        let url = start_server(state).await;
        let mut poller = HttpPoller::new(url);

        let events = poll_with_timeout(&mut poller).await;
        assert!(
            events.is_empty(),
            "expected no events on first 404, got {events:?}"
        );
    }

    // -----------------------------------------------------------------------
    // poll — Modified on subsequent 200 after content changes
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_modified_after_content_changes() {
        let state = MockState::default();
        *state.body.lock().unwrap() = Some("version-1".to_string());

        let url = start_server(state.clone()).await;
        let mut poller = HttpPoller::new(url);

        // First poll → Added
        let events = poll_with_timeout(&mut poller).await;
        assert!(matches!(events[0], ExternalEvent::Added(_)));

        // Mutate server content
        *state.body.lock().unwrap() = Some("version-2".to_string());

        // Second poll → Modified
        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(
            matches!(events[0], ExternalEvent::Modified(_)),
            "expected Modified after content change, got {:?}",
            events[0]
        );
        if let ExternalEvent::Modified(ref r) = events[0] {
            assert_eq!(r.body.as_ref(), b"version-2");
        }
    }

    // -----------------------------------------------------------------------
    // poll — 304 Not Modified when ETag matches
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_nothing_on_304_when_etag_unchanged() {
        let state = MockState::default();
        *state.body.lock().unwrap() = Some("content".to_string());
        *state.etag.lock().unwrap() = Some("\"abc123\"".to_string());

        let url = start_server(state).await;
        let mut poller = HttpPoller::new(url);

        // First poll → Added, poller records ETag
        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ExternalEvent::Added(_)));

        // Second poll → server sees matching If-None-Match → 304 → no events
        let events = poll_with_timeout(&mut poller).await;
        assert!(
            events.is_empty(),
            "expected no events on 304 Not Modified, got {events:?}"
        );
    }

    // -----------------------------------------------------------------------
    // poll — Removed when 404 arrives after resource was seen
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_emits_removed_when_resource_disappears() {
        let state = MockState::default();
        *state.body.lock().unwrap() = Some("present".to_string());

        let url = start_server(state.clone()).await;
        let mut poller = HttpPoller::new(url);

        // First poll → Added
        let events = poll_with_timeout(&mut poller).await;
        assert!(matches!(events[0], ExternalEvent::Added(_)));

        // Resource disappears
        *state.body.lock().unwrap() = None;

        // Second poll → Removed
        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(
            matches!(events[0], ExternalEvent::Removed(_)),
            "expected Removed after 404, got {:?}",
            events[0]
        );

        // Third poll → already removed, no further events
        let events = poll_with_timeout(&mut poller).await;
        assert!(
            events.is_empty(),
            "expected no events after removal was already reported, got {events:?}"
        );
    }

    // -----------------------------------------------------------------------
    // poll — ETag is exposed in the response struct
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn poll_response_contains_etag_from_server() {
        let state = MockState::default();
        *state.body.lock().unwrap() = Some("data".to_string());
        *state.etag.lock().unwrap() = Some("\"etag-value\"".to_string());

        let url = start_server(state).await;
        let mut poller = HttpPoller::new(url);

        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1);
        if let ExternalEvent::Added(ref r) = events[0] {
            assert_eq!(r.etag.as_deref(), Some("\"etag-value\""));
        } else {
            panic!("expected Added event");
        }
    }

    // -----------------------------------------------------------------------
    // with_bearer_token — Authorization header is forwarded
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn with_bearer_token_forwards_authorization_header() {
        async fn auth_handler(headers: HeaderMap) -> impl IntoResponse {
            let auth = headers
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if auth == "Bearer secret-token" {
                (StatusCode::OK, "authenticated").into_response()
            } else {
                StatusCode::UNAUTHORIZED.into_response()
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", get(auth_handler)))
                .await
                .unwrap();
        });

        let mut poller =
            HttpPoller::new(format!("http://{addr}/")).with_bearer_token("secret-token");

        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(
            matches!(events[0], ExternalEvent::Added(_)),
            "expected Added on authenticated 200, got {:?}",
            events[0]
        );
    }

    // -----------------------------------------------------------------------
    // with_header — custom header is forwarded
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn with_header_forwards_custom_header() {
        async fn key_handler(headers: HeaderMap) -> impl IntoResponse {
            let key = headers
                .get("x-api-key")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if key == "my-api-key" {
                (StatusCode::OK, "ok").into_response()
            } else {
                StatusCode::FORBIDDEN.into_response()
            }
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, Router::new().route("/", get(key_handler)))
                .await
                .unwrap();
        });

        let mut poller =
            HttpPoller::new(format!("http://{addr}/")).with_header("x-api-key", "my-api-key");

        let events = poll_with_timeout(&mut poller).await;
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], ExternalEvent::Added(_)));
    }
}
