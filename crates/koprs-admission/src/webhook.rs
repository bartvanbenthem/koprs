//! Admission webhook server framework.
//!
//! Provides [`Validator`] and [`WebhookBuilder`] — the two building blocks
//! needed to run a Kubernetes validating admission webhook server.
//!
//! [`WebhookBuilder`] handles the operational skeleton every production
//! webhook needs: TLS with certificate hot-reload, connection and request
//! timeouts, graceful connection draining, `/healthz`/`/readyz` HTTP probes,
//! and routing `POST` requests to typed validator implementations.
//! Implement [`Validator`] for your resource type and call
//! [`WebhookBuilder::run`] — everything else is automatic.
//!
//! # Quick start
//!
//! ```no_run
//! use koprs_admission::{AdmissionRequest, ValidationResponse};
//! use koprs_admission::webhook::{Validator, WebhookBuilder};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct MyResource {
//!     replicas: u32,
//! }
//!
//! struct ReplicaLimit;
//!
//! impl Validator<MyResource> for ReplicaLimit {
//!     type Error = std::convert::Infallible;
//!
//!     async fn validate(
//!         &self,
//!         request: &AdmissionRequest<MyResource>,
//!     ) -> Result<ValidationResponse, Self::Error> {
//!         let allowed = request
//!             .object
//!             .as_ref()
//!             .map_or(true, |r| r.replicas <= 10);
//!
//!         if allowed {
//!             Ok(ValidationResponse::allow())
//!         } else {
//!             Ok(ValidationResponse::deny("replicas must not exceed 10"))
//!         }
//!     }
//! }
//!
//! # async fn example() -> Result<(), koprs_admission::AdmissionError> {
//! WebhookBuilder::new()
//!     .port(8443)
//!     .tls_from_files("/tls/tls.crt", "/tls/tls.key")?
//!     .health_port(8080)
//!     .graceful_shutdown()
//!     .validate("/validate/myresource", ReplicaLimit)
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::{Json, Router, routing};
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use rustls::ServerConfig;
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;
use tracing::{debug, info, warn};

use crate::error::Result;
use crate::review::{
    AdmissionRequest, ValidationResponse, build_deny_response, build_response, parse_request,
    parse_uid,
};
use crate::tls;

// ---------------------------------------------------------------------------
// Validator trait
// ---------------------------------------------------------------------------

/// Trait that webhook authors implement to define validation logic.
///
/// `T` is the resource type being validated. It must be deserializable from
/// JSON (`serde::de::DeserializeOwned`) so the webhook framework can parse
/// the incoming `AdmissionReview` payload.
///
/// Returning `Ok(ValidationResponse::allow())` approves the resource.
/// Returning `Ok(ValidationResponse::deny("reason"))` rejects it with a
/// message shown to the user. Returning `Err(e)` causes the server to
/// respond with HTTP 500 so Kubernetes can apply the `failurePolicy`
/// configured on the `ValidatingWebhookConfiguration`.
///
/// # Implementing
///
/// ```no_run
/// use koprs_admission::{AdmissionRequest, ValidationResponse};
/// use koprs_admission::webhook::Validator;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct MyResource { name: String }
///
/// struct NoTestPrefix;
///
/// impl Validator<MyResource> for NoTestPrefix {
///     type Error = std::convert::Infallible;
///
///     async fn validate(
///         &self,
///         request: &AdmissionRequest<MyResource>,
///     ) -> Result<ValidationResponse, Self::Error> {
///         let denied = request
///             .object
///             .as_ref()
///             .map_or(false, |r| r.name.starts_with("test-"));
///
///         if denied {
///             Ok(ValidationResponse::deny("names beginning with 'test-' are reserved"))
///         } else {
///             Ok(ValidationResponse::allow())
///         }
///     }
/// }
/// ```
pub trait Validator<T>: Send + Sync + 'static
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    /// The error type returned when the validator itself fails (not when it
    /// decides to deny a resource). An `Err` here causes an HTTP 500
    /// response, allowing Kubernetes `failurePolicy` to kick in.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Validate the given admission request and return an allow or deny
    /// decision.
    ///
    /// The returned future must be `Send` because the webhook server
    /// dispatches requests on a multi-threaded executor.
    fn validate(
        &self,
        request: &AdmissionRequest<T>,
    ) -> impl Future<Output = std::result::Result<ValidationResponse, Self::Error>> + Send;
}

// ---------------------------------------------------------------------------
// Validator blanket impl for Arc<V>
// ---------------------------------------------------------------------------

impl<T, V> Validator<T> for std::sync::Arc<V>
where
    T: DeserializeOwned + Send + Sync + 'static,
    V: Validator<T>,
{
    type Error = V::Error;

    fn validate(
        &self,
        request: &AdmissionRequest<T>,
    ) -> impl Future<Output = std::result::Result<ValidationResponse, Self::Error>> + Send {
        (**self).validate(request)
    }
}

// ---------------------------------------------------------------------------
// Internal: type-erased handler
// ---------------------------------------------------------------------------

type BoxedHandler =
    Arc<dyn Fn(serde_json::Value) -> BoxFuture<'static, axum::response::Response> + Send + Sync>;

// ---------------------------------------------------------------------------
// Internal: core admission handler
// ---------------------------------------------------------------------------

/// Process a single `AdmissionReview` JSON body through a typed validator.
///
/// - Parse errors → deny with a descriptive message (400-style).
/// - Validator `Err` → HTTP 500 so `failurePolicy` applies.
/// - Validator `Ok(deny)` → deny with the validator's message.
/// - Validator `Ok(allow)` → allow.
#[cfg(test)]
pub(crate) async fn admit_test<T, V>(
    validator: &V,
    body: serde_json::Value,
) -> axum::response::Response
where
    T: DeserializeOwned + Send + Sync + 'static,
    V: Validator<T>,
{
    admit::<T, V>(validator, body).await
}

async fn admit<T, V>(validator: &V, body: serde_json::Value) -> axum::response::Response
where
    T: DeserializeOwned + Send + Sync + 'static,
    V: Validator<T>,
{
    use axum::response::IntoResponse;

    let uid = parse_uid(&body);

    let request = match parse_request::<T>(&body) {
        Ok(req) => req,
        Err(e) => {
            warn!(error = %e, "Failed to parse admission request");
            return Json(build_deny_response(&uid, &e.to_string())).into_response();
        }
    };

    match validator.validate(&request).await {
        Ok(resp) => Json(build_response(&uid, &resp)).into_response(),
        Err(e) => {
            warn!(error = %e, "Validator returned an error");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Internal: health server (same pattern as koprs::controller)
// ---------------------------------------------------------------------------

/// Serve HTTP/1.1 health probes on an already-bound listener.
///
/// `GET /healthz` — `200 OK` always (liveness).
/// `GET /readyz`  — `200 OK` once `ready` flips to `true`, else `503`.
pub(crate) async fn serve_health(listener: TcpListener, ready: Arc<AtomicBool>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            break;
        };
        let ready = ready.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(stream);
            let svc = service_fn(move |req: Request<Incoming>| {
                let ready = ready.clone();
                async move {
                    let (status, body): (StatusCode, &'static str) =
                        if req.uri().path() == "/readyz" {
                            if ready.load(Ordering::Acquire) {
                                (StatusCode::OK, "ok")
                            } else {
                                (StatusCode::SERVICE_UNAVAILABLE, "not ready")
                            }
                        } else {
                            (StatusCode::OK, "ok")
                        };
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(status)
                            .header("content-type", "text/plain")
                            .body(Full::new(Bytes::from_static(body.as_bytes())))
                            .unwrap(),
                    )
                }
            });
            let _ = http1::Builder::new().serve_connection(io, svc).await;
        });
    }
}

// ---------------------------------------------------------------------------
// Internal: graceful shutdown signal
// ---------------------------------------------------------------------------

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = sigterm.recv() => {},
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await;
}

// ---------------------------------------------------------------------------
// WebhookBuilder
// ---------------------------------------------------------------------------

/// Default limit on how long a single admission request may take.
///
/// The API server's own webhook timeout defaults to 10 s, so there is no point
/// in letting a validator run longer.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time a client may take to complete the TLS handshake.
pub(crate) const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time a client may take to send the request headers.
pub(crate) const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time to wait for in-flight requests to finish on shutdown.
pub(crate) const DRAIN_TIMEOUT: Duration = Duration::from_secs(15);
/// Maximum accepted `AdmissionReview` body size. Objects may be up to ~1.5 MiB
/// and UPDATE reviews carry both `object` and `oldObject`.
pub(crate) const MAX_BODY_BYTES: usize = 8 * 1024 * 1024;

/// Builder for a validating admission webhook server.
///
/// | Method | What it provides |
/// |--------|-----------------|
/// | `.port(port)` | Bind port for the webhook HTTPS/HTTP server |
/// | `.tls_from_files(cert, key)` | TLS from files, **reloaded automatically** on rotation |
/// | `.tls_from_pem(cert, key)` | TLS from in-memory PEM bytes (static) |
/// | `.tls(config)` | TLS using a pre-built `rustls::ServerConfig` |
/// | `.health_port(port)` | `GET /healthz` + `GET /readyz` server |
/// | `.graceful_shutdown()` | Drain in-flight requests on SIGTERM or Ctrl+C |
/// | `.request_timeout(dur)` | Upper bound on a single admission request (default 10 s) |
/// | `.validate(path, validator)` | Register a typed validator at `path` |
///
/// # TLS
///
/// Kubernetes requires admission webhooks to be served over HTTPS. Prefer
/// [`tls_from_files`](Self::tls_from_files) with the paths of a mounted
/// cert-manager `Secret`: the certificate is re-read when the files change,
/// so rotations need no pod restart. When no TLS is configured the server
/// listens on plain HTTP, which is useful when TLS is terminated by a sidecar
/// or service mesh.
///
/// # Hardening
///
/// Every connection gets a TLS handshake timeout and a header read timeout,
/// every request a [`request_timeout`](Self::request_timeout), and request
/// bodies are limited to 8 MiB, so slow or misbehaving clients cannot tie up
/// the server.
///
/// # Example
///
/// ```no_run
/// use koprs_admission::{AdmissionRequest, ValidationResponse};
/// use koprs_admission::webhook::{Validator, WebhookBuilder};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct MyResource { replicas: u32 }
///
/// struct ReplicaLimit;
///
/// impl Validator<MyResource> for ReplicaLimit {
///     type Error = std::convert::Infallible;
///
///     async fn validate(
///         &self,
///         request: &AdmissionRequest<MyResource>,
///     ) -> Result<ValidationResponse, Self::Error> {
///         if request.object.as_ref().map_or(true, |r| r.replicas <= 10) {
///             Ok(ValidationResponse::allow())
///         } else {
///             Ok(ValidationResponse::deny("replicas must not exceed 10"))
///         }
///     }
/// }
///
/// # async fn example() -> Result<(), koprs_admission::AdmissionError> {
/// WebhookBuilder::new()
///     .port(8443)
///     .tls_from_files("/tls/tls.crt", "/tls/tls.key")?
///     .health_port(8080)
///     .graceful_shutdown()
///     .validate("/validate/myresource", ReplicaLimit)
///     .run()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct WebhookBuilder {
    pub(crate) port: u16,
    pub(crate) tls: Option<Arc<ServerConfig>>,
    pub(crate) health_port: Option<u16>,
    pub(crate) graceful_shutdown: bool,
    pub(crate) request_timeout: Duration,
    pub(crate) routes: Vec<(String, BoxedHandler)>,
}

impl Default for WebhookBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl WebhookBuilder {
    /// Create a new builder with default settings.
    ///
    /// Defaults: port `8443`, no TLS, no health server, no graceful shutdown,
    /// 10 s request timeout.
    pub fn new() -> Self {
        Self {
            port: 8443,
            tls: None,
            health_port: None,
            graceful_shutdown: false,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            routes: vec![],
        }
    }

    /// Set the port the webhook server listens on (default: `8443`).
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Configure TLS using a pre-built [`rustls::ServerConfig`].
    ///
    /// Use this when you need full control over the TLS configuration
    /// (client auth, custom cipher suites, ALPN). For a mounted cert-manager
    /// certificate, [`tls_from_files`][Self::tls_from_files] is simpler and
    /// handles rotation.
    pub fn tls(mut self, config: Arc<ServerConfig>) -> Self {
        self.tls = Some(config);
        self
    }

    /// Configure TLS from PEM-encoded certificate and private key bytes.
    ///
    /// The certificate is fixed for the lifetime of the server; use
    /// [`tls_from_files`][Self::tls_from_files] if it can be rotated.
    /// Supports RSA, ECDSA and Ed25519 keys. Returns an error if the PEM data
    /// is malformed, contains no usable certificate/key pair, or the key does
    /// not match the certificate.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use koprs_admission::webhook::WebhookBuilder;
    ///
    /// # fn example() -> Result<(), koprs_admission::AdmissionError> {
    /// let cert_pem = std::fs::read("/tls/tls.crt")?;
    /// let key_pem  = std::fs::read("/tls/tls.key")?;
    /// let builder = WebhookBuilder::new().tls_from_pem(&cert_pem, &key_pem)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tls_from_pem(mut self, cert_pem: &[u8], key_pem: &[u8]) -> Result<Self> {
        let key = tls::certified_key_from_pem(cert_pem, key_pem, &tls::provider())?;
        let config = tls::server_config(Arc::new(tls::StaticCertResolver(Arc::new(key))))?;
        self.tls = Some(Arc::new(config));
        Ok(self)
    }

    /// Configure TLS from certificate and key files, reloading them when they
    /// change on disk.
    ///
    /// Point this at a mounted cert-manager `Secret` (e.g. `/tls/tls.crt` and
    /// `/tls/tls.key`). The files are checked for changes at most every 10 s;
    /// a rotated certificate is picked up on the next TLS handshake without
    /// restarting the pod. If a reload fails, the previous certificate keeps
    /// being served.
    ///
    /// Returns an error if the files cannot be read or do not contain a
    /// matching certificate/key pair at startup.
    pub fn tls_from_files(
        self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
    ) -> Result<Self> {
        self.tls_from_files_with_interval(cert_path, key_path, tls::DEFAULT_RELOAD_INTERVAL)
    }

    /// Like [`tls_from_files`](Self::tls_from_files) with a custom reload
    /// check interval.
    pub fn tls_from_files_with_interval(
        mut self,
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        reload_interval: Duration,
    ) -> Result<Self> {
        let resolver = tls::ReloadingCertResolver::new(cert_path, key_path, reload_interval)?;
        self.tls = Some(Arc::new(tls::server_config(Arc::new(resolver))?));
        Ok(self)
    }

    /// Start a health probe HTTP server on `0.0.0.0:<port>`.
    ///
    /// `GET /healthz` always returns `200 OK`.
    /// `GET /readyz` returns `200 OK` while the webhook listener is bound and
    /// serving, and `503` before that and after a shutdown signal (so the pod
    /// is removed from the Service endpoints while it drains).
    pub fn health_port(mut self, port: u16) -> Self {
        self.health_port = Some(port);
        self
    }

    /// Stop the server cleanly on SIGTERM or Ctrl+C.
    ///
    /// On the signal, `/readyz` switches to `503`, new connections are no
    /// longer accepted, and in-flight requests are given up to 15 s to finish
    /// before [`run`](Self::run) returns.
    pub fn graceful_shutdown(mut self) -> Self {
        self.graceful_shutdown = true;
        self
    }

    /// Upper bound on how long one admission request may take (default 10 s).
    ///
    /// A validator that exceeds it gets an HTTP 500 response, so the
    /// webhook's `failurePolicy` applies.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Register a typed [`Validator`] to handle `POST` requests at `path`.
    ///
    /// The path must match the `clientConfig.service.path` configured on the
    /// `ValidatingWebhookConfiguration` (e.g. `/validate/myresource`).
    ///
    /// Multiple validators can be registered at different paths by chaining
    /// calls to `.validate()`. Each call adds a route to the same server.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use koprs_admission::{AdmissionRequest, ValidationResponse};
    /// use koprs_admission::webhook::{Validator, WebhookBuilder};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct MyResource { name: String }
    ///
    /// struct AlwaysAllow;
    ///
    /// impl Validator<MyResource> for AlwaysAllow {
    ///     type Error = std::convert::Infallible;
    ///     async fn validate(&self, _: &AdmissionRequest<MyResource>)
    ///         -> Result<ValidationResponse, Self::Error>
    ///     {
    ///         Ok(ValidationResponse::allow())
    ///     }
    /// }
    ///
    /// # fn example() {
    /// let _builder = WebhookBuilder::new()
    ///     .validate("/validate/myresource", AlwaysAllow);
    /// # }
    /// ```
    pub fn validate<T, V>(mut self, path: impl Into<String>, validator: V) -> Self
    where
        T: DeserializeOwned + Send + Sync + 'static,
        V: Validator<T>,
    {
        let validator = Arc::new(validator);
        let handler: BoxedHandler = Arc::new(move |body: serde_json::Value| {
            let v = validator.clone();
            Box::pin(async move { admit::<T, V>(&v, body).await })
        });
        self.routes.push((path.into(), handler));
        self
    }

    /// Build the axum router for the registered validators.
    pub(crate) fn router(routes: Vec<(String, BoxedHandler)>, request_timeout: Duration) -> Router {
        use axum::response::IntoResponse;

        let mut router = Router::new();
        for (path, handler) in routes {
            router = router.route(
                &path,
                routing::post(move |Json(body): Json<serde_json::Value>| {
                    let handler = handler.clone();
                    async move {
                        match tokio::time::timeout(request_timeout, handler(body)).await {
                            Ok(response) => response,
                            Err(_) => {
                                warn!(timeout = ?request_timeout, "Admission request timed out");
                                StatusCode::INTERNAL_SERVER_ERROR.into_response()
                            }
                        }
                    }
                }),
            );
        }
        router.layer(DefaultBodyLimit::max(MAX_BODY_BYTES))
    }

    /// Start the webhook server.
    ///
    /// In order:
    /// 1. Binds the health server if `.health_port()` was set.
    /// 2. Binds the webhook listener on the configured port.
    /// 3. Marks `/readyz` healthy.
    /// 4. Serves requests — with TLS if configured, plain HTTP otherwise.
    /// 5. On a shutdown signal (if `.graceful_shutdown()` was set), stops
    ///    accepting connections and drains in-flight requests.
    pub async fn run(self) -> Result<()> {
        let WebhookBuilder {
            port,
            tls,
            health_port,
            graceful_shutdown,
            request_timeout,
            routes,
        } = self;

        let router = Self::router(routes, request_timeout);

        // --- Health server ---
        let ready = Arc::new(AtomicBool::new(false));
        if let Some(hp) = health_port {
            let listener = TcpListener::bind(("0.0.0.0", hp)).await?;
            info!(port = hp, "Health server listening");
            tokio::spawn(serve_health(listener, ready.clone()));
        }

        // --- Shutdown signal ---
        // `_shutdown_tx` lives until `run` returns, so the receiver never
        // observes a closed channel when graceful shutdown is disabled.
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        if graceful_shutdown {
            let tx = _shutdown_tx.clone();
            let ready = ready.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                info!("Shutdown signal received");
                ready.store(false, Ordering::Release);
                tx.send(true).ok();
            });
        }

        // --- Bind webhook port ---
        let listener = TcpListener::bind(("0.0.0.0", port)).await?;
        info!(port, tls = tls.is_some(), "Webhook server listening");
        ready.store(true, Ordering::Release);

        serve(listener, tls.map(TlsAcceptor::from), router, shutdown_rx).await;
        Ok(())
    }
}

/// Accept connections until `shutdown` flips to `true`, then drain.
pub(crate) async fn serve(
    listener: TcpListener,
    acceptor: Option<TlsAcceptor>,
    router: Router,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let graceful = GracefulShutdown::new();

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(conn) => conn,
                    Err(e) => {
                        // Typically EMFILE / ECONNABORTED: transient, keep serving.
                        warn!(error = %e, "Failed to accept connection");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let router = router.clone();
                let acceptor = acceptor.clone();
                let watcher = graceful.watcher();
                tokio::spawn(async move {
                    match acceptor {
                        Some(acc) => {
                            match tokio::time::timeout(TLS_HANDSHAKE_TIMEOUT, acc.accept(stream)).await {
                                Ok(Ok(tls_stream)) => {
                                    serve_connection(TokioIo::new(tls_stream), router, watcher).await;
                                }
                                Ok(Err(e)) => debug!(%peer, error = %e, "TLS handshake failed"),
                                Err(_) => debug!(%peer, "TLS handshake timed out"),
                            }
                        }
                        None => serve_connection(TokioIo::new(stream), router, watcher).await,
                    }
                });
            }
            // Map to `bool` so the watch read guard is not held across awaits
            // (it is `!Send`).
            stop = async { shutdown.wait_for(|stop| *stop).await.is_ok() } => {
                if stop {
                    break;
                }
            }
        }
    }

    drop(listener);
    info!("Webhook server draining in-flight requests");
    if tokio::time::timeout(DRAIN_TIMEOUT, graceful.shutdown())
        .await
        .is_err()
    {
        warn!(timeout = ?DRAIN_TIMEOUT, "Drain timed out; closing remaining connections");
    }
}

async fn serve_connection<I>(io: I, router: Router, watcher: Watcher)
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let svc = service_fn(move |req: Request<Incoming>| {
        let router = router.clone();
        async move { router.oneshot(req.map(Body::new)).await }
    });
    let conn = http1::Builder::new()
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .serve_connection(io, svc);
    if let Err(e) = watcher.watch(conn).await {
        debug!(error = %e, "Connection closed");
    }
}
