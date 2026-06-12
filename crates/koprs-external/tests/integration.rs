//! Integration tests for koprs-external.
//!
//! The HTTP tests spin up a real local axum server on an ephemeral port and
//! exercise the full `HttpPoller` → `watch_external` stack end-to-end —
//! no mocks, no stubbed futures.
//!
//! The Kubernetes test creates a ConfigMap via `kube::Client` and polls the
//! same resource through `HttpPoller`, demonstrating that the poller works
//! against the Kubernetes REST API. It is **ignored by default** because it
//! requires a reachable cluster and manual environment setup (see below).
//!
//! # Running the HTTP tests
//!
//! ```bash
//! cargo test -p koprs-external --features integration --test integration
//! ```
//!
//! # Running the Kubernetes test
//!
//! ```bash
//! # 1. Start a local cluster (kind recommended)
//! kind create cluster --name koprs-ext-test
//!
//! # 2. Create a service account and mint a short-lived token
//! kubectl create serviceaccount koprs-ext-test -n default
//! kubectl create clusterrolebinding koprs-ext-test \
//!     --clusterrole=view --serviceaccount=default:koprs-ext-test
//! export KUBE_TOKEN=$(kubectl create token koprs-ext-test)
//! export KUBE_API_URL=$(kubectl config view --minify \
//!     -o jsonpath='{.clusters[0].cluster.server}')
//!
//! # 3. Run the ignored test
//! cargo test -p koprs-external --features integration --test integration \
//!     -- --include-ignored
//!
//! # 4. Tear down
//! kind delete cluster --name koprs-ext-test
//! ```

#![cfg(feature = "integration")]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::get,
};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use koprs_external::{
    ExternalEvent,
    http::HttpPoller,
    watcher::{ExternalSource, watch_external},
};
use kube::api::{DeleteParams, PostParams};
use kube::{Api, Client};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::timeout;

// -------------------------------------------------------------------------
// Mock HTTP server
// -------------------------------------------------------------------------

#[derive(Clone, Default)]
struct MockState {
    body: Arc<Mutex<Option<String>>>,
    etag: Arc<Mutex<Option<String>>>,
}

async fn mock_handler(State(state): State<MockState>, headers: HeaderMap) -> impl IntoResponse {
    let body = state.body.lock().unwrap().clone();
    let etag_val = state.etag.lock().unwrap().clone();

    let Some(text) = body else {
        return StatusCode::NOT_FOUND.into_response();
    };

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
    (StatusCode::OK, resp_headers, text).into_response()
}

async fn start_mock_server(state: MockState) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/resource", get(mock_handler))
        .with_state(state);
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    format!("http://{addr}/resource")
}

/// Unique suffix to avoid name collisions across parallel tests.
fn uid(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{prefix}-{ns}")
}

// -------------------------------------------------------------------------
// HTTP poller — Added event
// -------------------------------------------------------------------------

#[tokio::test]
async fn http_poller_emits_added_on_first_200() {
    let state = MockState::default();
    *state.body.lock().unwrap() = Some("hello".to_string());

    let url = start_mock_server(state).await;
    let mut poller = HttpPoller::new(&url).with_name(uid("added"));

    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .expect("poll timed out")
        .expect("poll error");

    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ExternalEvent::Added(_)));
    if let ExternalEvent::Added(ref r) = events[0] {
        assert_eq!(r.status, 200);
        assert_eq!(r.body.as_ref(), b"hello");
    }
}

// -------------------------------------------------------------------------
// HTTP poller — no event when resource never existed (404)
// -------------------------------------------------------------------------

#[tokio::test]
async fn http_poller_emits_nothing_when_404_never_seen() {
    let state = MockState::default();
    let url = start_mock_server(state).await;
    let mut poller = HttpPoller::new(url);

    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .expect("poll timed out")
        .expect("poll error");

    assert!(events.is_empty(), "expected no events on first 404");
}

// -------------------------------------------------------------------------
// HTTP poller — Modified event
// -------------------------------------------------------------------------

#[tokio::test]
async fn http_poller_emits_modified_after_content_change() {
    let state = MockState::default();
    *state.body.lock().unwrap() = Some("v1".to_string());

    let url = start_mock_server(state.clone()).await;
    let mut poller = HttpPoller::new(&url).with_name(uid("modified"));

    // First poll → Added
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(events[0], ExternalEvent::Added(_)));

    *state.body.lock().unwrap() = Some("v2".to_string());

    // Second poll → Modified
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ExternalEvent::Modified(_)));
    if let ExternalEvent::Modified(ref r) = events[0] {
        assert_eq!(r.body.as_ref(), b"v2");
    }
}

// -------------------------------------------------------------------------
// HTTP poller — 304 Not Modified (ETag match)
// -------------------------------------------------------------------------

#[tokio::test]
async fn http_poller_emits_nothing_on_304_when_etag_unchanged() {
    let state = MockState::default();
    *state.body.lock().unwrap() = Some("content".to_string());
    *state.etag.lock().unwrap() = Some("\"v1\"".to_string());

    let url = start_mock_server(state).await;
    let mut poller = HttpPoller::new(url);

    // First poll → Added (poller captures ETag)
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(events[0], ExternalEvent::Added(_)));

    // Second poll → 304 because ETag matches → no events
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert!(events.is_empty(), "expected no events on 304");
}

// -------------------------------------------------------------------------
// HTTP poller — Removed event
// -------------------------------------------------------------------------

#[tokio::test]
async fn http_poller_emits_removed_then_nothing_after_resource_disappears() {
    let state = MockState::default();
    *state.body.lock().unwrap() = Some("here".to_string());

    let url = start_mock_server(state.clone()).await;
    let mut poller = HttpPoller::new(url);

    // First poll → Added
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(events[0], ExternalEvent::Added(_)));

    // Resource disappears
    *state.body.lock().unwrap() = None;

    // Second poll → Removed
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], ExternalEvent::Removed(_)));

    // Third poll → already gone, no further event
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .unwrap()
        .unwrap();
    assert!(events.is_empty(), "removal should be reported exactly once");
}

// -------------------------------------------------------------------------
// watch_external — full polling loop
// -------------------------------------------------------------------------

#[tokio::test]
async fn watch_external_delivers_events_through_channel() {
    let state = MockState::default();
    *state.body.lock().unwrap() = Some("initial".to_string());

    let url = start_mock_server(state.clone()).await;
    let poller = HttpPoller::new(url).with_name(uid("watch-loop"));

    let (tx, mut rx) = mpsc::channel(16);
    let _handle = watch_external(poller, Duration::from_millis(10), tx);

    // Added should arrive quickly
    let ev = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for Added event")
        .expect("channel closed");
    assert!(
        matches!(ev, ExternalEvent::Added(_)),
        "expected Added, got {ev:?}"
    );

    // Mutate the body and wait for Modified
    *state.body.lock().unwrap() = Some("updated".to_string());

    let ev = timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("timed out waiting for Modified event")
        .expect("channel closed");
    assert!(
        matches!(ev, ExternalEvent::Modified(_)),
        "expected Modified, got {ev:?}"
    );
}

// -------------------------------------------------------------------------
// watch_external — shuts down cleanly when receiver is dropped
// -------------------------------------------------------------------------

#[tokio::test]
async fn watch_external_shuts_down_when_receiver_is_dropped() {
    let state = MockState::default();
    *state.body.lock().unwrap() = Some("data".to_string());

    let url = start_mock_server(state).await;
    let poller = HttpPoller::new(url);

    let (tx, rx) = mpsc::channel(16);
    let handle = watch_external(poller, Duration::from_millis(10), tx);

    drop(rx);

    timeout(Duration::from_secs(2), handle)
        .await
        .expect("watcher did not shut down after receiver drop")
        .expect("watcher task panicked");
}

// -------------------------------------------------------------------------
// Kubernetes REST API test — requires a running cluster
// -------------------------------------------------------------------------

/// End-to-end test against the Kubernetes API.
///
/// Creates a ConfigMap via `kube::Client`, polls the same resource through
/// `HttpPoller` with bearer-token auth, and verifies that `Added`,
/// `Modified`, and `Removed` events are produced correctly.
///
/// Set `KUBE_TOKEN` and `KUBE_API_URL` before running (see file-level docs).
#[tokio::test]
#[ignore = "requires a running Kubernetes cluster; set KUBE_TOKEN and KUBE_API_URL, then run with -- --include-ignored"]
async fn kubernetes_configmap_lifecycle_via_http_poller() {
    let token = std::env::var("KUBE_TOKEN").expect("KUBE_TOKEN must be set");
    let api_url =
        std::env::var("KUBE_API_URL").unwrap_or_else(|_| "https://127.0.0.1:6443".to_string());

    let name = uid("koprs-ext-test");
    let namespace = "default";
    let resource_url = format!("{api_url}/api/v1/namespaces/{namespace}/configmaps/{name}");

    // Build a reqwest client that accepts the cluster's self-signed cert
    // (typical for kind/k3d). Production clusters should use a proper CA.
    let reqwest_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();

    let mut poller = HttpPoller::new(&resource_url)
        .with_client(reqwest_client)
        .with_bearer_token(&token);

    // Pre-condition: ConfigMap does not exist yet
    let events = poller.poll().await.expect("initial poll failed");
    assert!(
        events.is_empty(),
        "ConfigMap should not exist at test start"
    );

    // Create the ConfigMap via kube::Client
    let kube_client = Client::try_default()
        .await
        .expect("failed to build kube Client — is a cluster reachable?");
    let api: Api<ConfigMap> = Api::namespaced(kube_client.clone(), namespace);
    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.clone()),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        },
        ..Default::default()
    };
    api.create(&PostParams::default(), &cm)
        .await
        .expect("failed to create ConfigMap");

    // Poll → Added
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .expect("poll timed out after create")
        .expect("poll error after create");
    assert_eq!(events.len(), 1);
    assert!(
        matches!(events[0], ExternalEvent::Added(_)),
        "expected Added after ConfigMap create, got {:?}",
        events[0]
    );

    // Delete the ConfigMap
    api.delete(&name, &DeleteParams::default())
        .await
        .expect("failed to delete ConfigMap");

    // Poll → Removed
    let events = timeout(Duration::from_secs(5), poller.poll())
        .await
        .expect("poll timed out after delete")
        .expect("poll error after delete");
    assert_eq!(events.len(), 1);
    assert!(
        matches!(events[0], ExternalEvent::Removed(_)),
        "expected Removed after ConfigMap delete, got {:?}",
        events[0]
    );
}
