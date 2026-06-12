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
}

impl HttpPoller {
    /// Create a new poller for the given URL using a default [`reqwest::Client`].
    ///
    /// Call [`with_client`][Self::with_client] to bring a pre-configured
    /// client (e.g. one with custom CA certificates or timeouts).
    pub fn new(url: impl Into<String>) -> Self {
        let url = url.into();
        Self {
            name: url.clone(),
            client: Client::new(),
            url,
            extra_headers: Vec::new(),
            last_etag: None,
            last_modified_value: None,
            seen: false,
        }
    }

    /// Override the display name used in log output (default: the URL).
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Replace the underlying HTTP client.
    ///
    /// Use this to apply custom TLS certificates, proxy settings, or
    /// connection timeouts. The client must have `rustls-tls` or
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

impl ExternalSource for HttpPoller {
    type Item = HttpResponse;

    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<HttpResponse>>>> {
        let this = self;
        Box::pin(async move {
            let mut req = this.client.get(&this.url);

            // Conditional GET to avoid re-processing unchanged responses.
            // Prefer ETag over Last-Modified when both are available.
            if let Some(etag) = &this.last_etag {
                req = req.header(IF_NONE_MATCH, etag.as_str());
            } else if let Some(lm) = &this.last_modified_value {
                req = req.header(IF_MODIFIED_SINCE, lm.as_str());
            }

            for (key, value) in &this.extra_headers {
                let name: HeaderName =
                    key.parse()
                        .map_err(|e: reqwest::header::InvalidHeaderName| {
                            ExternalError::Internal(e.to_string())
                        })?;
                let val: HeaderValue =
                    value
                        .parse()
                        .map_err(|e: reqwest::header::InvalidHeaderValue| {
                            ExternalError::Internal(e.to_string())
                        })?;
                req = req.header(name, val);
            }

            let response = req.send().await?;

            match response.status() {
                StatusCode::NOT_MODIFIED => {
                    debug!(url = %this.url, "Not modified (304)");
                    Ok(vec![])
                }

                StatusCode::NOT_FOUND => {
                    if this.seen {
                        debug!(url = %this.url, "Resource removed (404)");
                        this.seen = false;
                        this.last_etag = None;
                        this.last_modified_value = None;
                        Ok(vec![ExternalEvent::Removed(HttpResponse {
                            body: Bytes::new(),
                            url: this.url.clone(),
                            status: 404,
                            etag: None,
                            last_modified: None,
                            content_type: None,
                        })])
                    } else {
                        Ok(vec![])
                    }
                }

                s if s.is_success() => {
                    let etag = response
                        .headers()
                        .get(ETAG)
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let last_modified = response
                        .headers()
                        .get(LAST_MODIFIED)
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let content_type = response
                        .headers()
                        .get(CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .map(String::from);
                    let status = response.status().as_u16();
                    let body = response.bytes().await?;

                    let http_response = HttpResponse {
                        body,
                        url: this.url.clone(),
                        status,
                        etag: etag.clone(),
                        last_modified: last_modified.clone(),
                        content_type,
                    };

                    let event = if this.seen {
                        ExternalEvent::Modified(http_response)
                    } else {
                        this.seen = true;
                        ExternalEvent::Added(http_response)
                    };

                    this.last_etag = etag;
                    this.last_modified_value = last_modified;

                    Ok(vec![event])
                }

                s => Err(ExternalError::Internal(format!(
                    "Unexpected HTTP status: {s}"
                ))),
            }
        })
    }
}
