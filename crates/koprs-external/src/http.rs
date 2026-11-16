//! HTTP API poller.
//!
//! [`HttpPoller`] polls a single HTTP endpoint and emits [`ExternalEvent`]s
//! when the response changes. Change detection uses `ETag` / `304 Not Modified`
//! when the server supports conditional requests, falling back to
//! `Last-Modified` otherwise.
//!
//! - The **first** successful response produces [`ExternalEvent::Added`].
//! - A subsequent response with a different body (no `ETag` match) produces
//!   [`ExternalEvent::Modified`].
//! - A subsequent `200` whose body is byte-for-byte the same as the previous
//!   one produces no event, so endpoints without `ETag` / `Last-Modified`
//!   support do not report a change on every poll.
//! - A `404 Not Found` after a prior success produces
//!   [`ExternalEvent::Removed`].
//! - `304 Not Modified` produces no event.
//!
//! # Authentication
//!
//! Pass a bearer token with [`HttpPoller::with_bearer_token`]. Arbitrary
//! request headers (API keys, custom auth) are added via
//! [`HttpPoller::with_header`]. Bring your own pre-configured
//! [`reqwest::Client`] via [`HttpPoller::with_client`] for mutual TLS,
//! custom timeouts, or connection pool settings.
//!
//! # Timeouts
//!
//! The default client applies a [`DEFAULT_REQUEST_TIMEOUT`] (30 s) per request
//! and a [`DEFAULT_CONNECT_TIMEOUT`] (10 s), so an endpoint that accepts the
//! connection but never answers cannot stall the watcher forever. A client
//! passed to [`HttpPoller::with_client`] is used as-is — configure timeouts on
//! it yourself.

use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use reqwest::{
    Client, StatusCode,
    header::{
        AUTHORIZATION, CONTENT_TYPE, ETAG, HeaderName, HeaderValue, IF_MODIFIED_SINCE,
        IF_NONE_MATCH, LAST_MODIFIED,
    },
};
use tracing::debug;

use crate::{
    error::{ExternalError, Result},
    watcher::{ExternalEvent, ExternalSource},
};

/// Total per-request timeout of the default client built by [`HttpPoller::new`].
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Connect timeout of the default client built by [`HttpPoller::new`].
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// The default client: bounded request and connect timeouts.
fn default_client(timeout: Duration) -> Client {
    Client::builder()
        .timeout(timeout)
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT.min(timeout))
        .build()
        // Same failure mode as `reqwest::Client::new()`, which also panics if
        // the TLS backend cannot be initialised.
        .expect("failed to build HTTP client")
}

/// Read a response body, enforcing an optional size limit while streaming.
async fn read_body(
    mut response: reqwest::Response,
    max: Option<usize>,
    url: &str,
) -> Result<Bytes> {
    let Some(max) = max else {
        return Ok(response.bytes().await?);
    };
    let too_large =
        || ExternalError::Internal(format!("response body from {url} exceeds {max} bytes"));
    if response
        .content_length()
        .is_some_and(|len| len > max as u64)
    {
        return Err(too_large());
    }
    let mut buf = bytes::BytesMut::new();
    while let Some(chunk) = response.chunk().await? {
        if buf.len() + chunk.len() > max {
            return Err(too_large());
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf.freeze())
}

// ---------------------------------------------------------------------------
// HttpResponse
// ---------------------------------------------------------------------------

/// The HTTP response body and selected headers returned by [`HttpPoller`].
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// Raw response body.
    pub body: Bytes,
    /// URL that was polled.
    pub url: String,
    /// HTTP status code.
    pub status: u16,
    /// `ETag` response header, if present.
    pub etag: Option<String>,
    /// `Last-Modified` response header, if present.
    pub last_modified: Option<String>,
    /// `Content-Type` response header, if present.
    pub content_type: Option<String>,
}

// ---------------------------------------------------------------------------
// HttpPoller
// ---------------------------------------------------------------------------

/// Polls a single HTTP endpoint and emits change events.
///
/// Build with [`HttpPoller::new`], optionally configure via the builder
/// methods, then pass to [`watch_external`][crate::watcher::watch_external]
/// or call [`ExternalSource::poll`] directly for manual control.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
/// use koprs_external::http::HttpPoller;
/// use koprs_external::watcher::{watch_external, ExternalEvent};
/// use tokio::sync::mpsc;
///
/// # #[tokio::main]
/// # async fn main() {
/// let (tx, mut rx) = mpsc::channel(16);
/// let poller = HttpPoller::new("https://api.example.com/resource")
///     .with_bearer_token("my-token");
///
/// let _handle = watch_external(poller, Duration::from_secs(30), tx);
///
/// while let Some(event) = rx.recv().await {
///     match event {
///         ExternalEvent::Added(r)    => println!("added:   {} bytes", r.body.len()),
///         ExternalEvent::Modified(r) => println!("changed: {} bytes", r.body.len()),
///         ExternalEvent::Removed(_)  => println!("resource gone"),
///     }
/// }
/// # }
/// ```
pub struct HttpPoller {
    client: Client,
    url: String,
    name: String,
    extra_headers: Vec<(String, String)>,
    last_etag: Option<String>,
    last_modified_value: Option<String>,
    seen: bool,
    /// Body of the last successful response, for change detection against
    /// servers that answer `200` without validators. `Bytes` is reference
    /// counted, so this shares the allocation handed out in the event.
    last_body: Option<Bytes>,
    max_body_bytes: Option<usize>,
}

impl HttpPoller {
    /// Create a new poller for the given URL using a default [`reqwest::Client`]
    /// with a 30 s request timeout and 10 s connect timeout.
    ///
    /// Call [`with_timeout`][Self::with_timeout] to change the timeout, or
    /// [`with_client`][Self::with_client] to bring a pre-configured client
    /// (e.g. one with custom CA certificates or connection pooling shared
    /// across pollers).
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            name: url.clone(),
            client: default_client(DEFAULT_REQUEST_TIMEOUT),
            url,
            extra_headers: Vec::new(),
            last_etag: None,
            last_modified_value: None,
            seen: false,
            last_body: None,
            max_body_bytes: None,
        }
    }

    /// Override the display name used in log output (default: the URL).
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Reject responses whose body exceeds `max` bytes (default: unlimited).
    ///
    /// The limit is enforced while streaming, so an oversized or endless
    /// response is cut off instead of being buffered in memory. A response
    /// over the limit makes [`poll`](ExternalSource::poll) return an error.
    pub fn with_max_body_size(mut self, max: usize) -> Self {
        self.max_body_bytes = Some(max);
        self
    }

    /// Replace the default client with one using a different per-request
    /// timeout (connect timeout stays at most 10 s).
    ///
    /// Replaces any client set earlier with [`with_client`](Self::with_client).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = default_client(timeout);
        self
    }

    /// Replace the underlying HTTP client.
    ///
    /// Use this to apply custom TLS certificates, proxy settings, or to share
    /// one connection pool across many pollers. The client is used as-is:
    /// **configure a request timeout on it**, otherwise a hung endpoint can
    /// block the poll indefinitely. The client must have `rustls-tls` or
    /// `native-tls` enabled to handle HTTPS endpoints.
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Append an arbitrary HTTP header to every request.
    ///
    /// Call multiple times to add multiple headers.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extra_headers.push((key.into(), value.into()));
        self
    }

    /// Add an `Authorization: Bearer <token>` header to every request.
    pub fn with_bearer_token(self, token: impl AsRef<str>) -> Self {
        self.with_header(AUTHORIZATION.as_str(), format!("Bearer {}", token.as_ref()))
    }
}

/// A response header as text, if present and valid UTF-8.
fn header_text(response: &reqwest::Response, name: HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(String::from)
}

impl HttpPoller {
    /// The conditional GET for this poll, with the configured headers.
    fn build_request(&self) -> Result<reqwest::RequestBuilder> {
        let mut request = self.client.get(&self.url);

        // Conditional GET to avoid re-processing unchanged responses.
        // Prefer ETag over Last-Modified when both are available.
        if let Some(etag) = &self.last_etag {
            request = request.header(IF_NONE_MATCH, etag.as_str());
        } else if let Some(last_modified) = &self.last_modified_value {
            request = request.header(IF_MODIFIED_SINCE, last_modified.as_str());
        }

        for (key, value) in &self.extra_headers {
            let name: HeaderName =
                key.parse()
                    .map_err(|e: reqwest::header::InvalidHeaderName| {
                        ExternalError::Internal(e.to_string())
                    })?;
            let mut value: HeaderValue =
                value
                    .parse()
                    .map_err(|e: reqwest::header::InvalidHeaderValue| {
                        ExternalError::Internal(e.to_string())
                    })?;
            // Keep credentials out of `Debug` output and request logs.
            if name == AUTHORIZATION {
                value.set_sensitive(true);
            }
            request = request.header(name, value);
        }
        Ok(request)
    }

    /// `404`: report the removal once, then forget everything about the resource.
    fn on_not_found(&mut self) -> Vec<ExternalEvent<HttpResponse>> {
        if !self.seen {
            return vec![];
        }
        debug!(url = %self.url, "Resource removed (404)");
        self.seen = false;
        self.last_etag = None;
        self.last_modified_value = None;
        self.last_body = None;
        vec![ExternalEvent::Removed(HttpResponse {
            body: Bytes::new(),
            url: self.url.clone(),
            status: StatusCode::NOT_FOUND.as_u16(),
            etag: None,
            last_modified: None,
            content_type: None,
        })]
    }

    /// `2xx`: read the body and report it as added, modified or unchanged.
    async fn on_success(
        &mut self,
        response: reqwest::Response,
    ) -> Result<Vec<ExternalEvent<HttpResponse>>> {
        let etag = header_text(&response, ETAG);
        let last_modified = header_text(&response, LAST_MODIFIED);
        let content_type = header_text(&response, CONTENT_TYPE);
        let status = response.status().as_u16();
        // State is only advanced after the body was read in full, so a
        // truncated or oversized response is retried from the old state.
        let body = read_body(response, self.max_body_bytes, &self.url).await?;

        let is_unchanged = self.seen && self.last_body.as_ref() == Some(&body);
        self.last_etag = etag.clone();
        self.last_modified_value = last_modified.clone();
        if is_unchanged {
            debug!(url = %self.url, "Body unchanged since the previous poll");
            return Ok(vec![]);
        }
        self.last_body = Some(body.clone());

        let http_response = HttpResponse {
            body,
            url: self.url.clone(),
            status,
            etag,
            last_modified,
            content_type,
        };
        if self.seen {
            return Ok(vec![ExternalEvent::Modified(http_response)]);
        }
        self.seen = true;
        Ok(vec![ExternalEvent::Added(http_response)])
    }

    async fn poll_once(&mut self) -> Result<Vec<ExternalEvent<HttpResponse>>> {
        let response = self.build_request()?.send().await?;
        match response.status() {
            StatusCode::NOT_MODIFIED => {
                debug!(url = %self.url, "Not modified (304)");
                Ok(vec![])
            }
            StatusCode::NOT_FOUND => Ok(self.on_not_found()),
            status if status.is_success() => self.on_success(response).await,
            status => Err(ExternalError::Internal(format!(
                "Unexpected HTTP status: {status}"
            ))),
        }
    }
}

impl ExternalSource for HttpPoller {
    type Item = HttpResponse;

    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<HttpResponse>>>> {
        Box::pin(self.poll_once())
    }
}
