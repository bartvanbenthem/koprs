//! Admission webhook server framework.
//!
//! Provides [`Validator`] and [`WebhookBuilder`] — the two building blocks
//! needed to run a Kubernetes validating admission webhook server.
//!
//! [`WebhookBuilder`] handles the operational skeleton every production
//! webhook needs: TLS, graceful shutdown, `/healthz`/`/readyz` HTTP probes,
//! and routing `POST` requests to typed validator implementations.
//! Implement [`Validator`] for your resource type and call
//! [`WebhookBuilder::run`] — everything else is automatic.
//!
//! # Quick start
//!
//! ```no_run
//! use std::fs;
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
//! let cert_pem = fs::read("/tls/tls.crt")?;
//! let key_pem  = fs::read("/tls/tls.key")?;
//!
//! WebhookBuilder::new()
//!     .port(8443)
//!     .tls_from_pem(&cert_pem, &key_pem)?
//!     .health_port(8080)
//!     .graceful_shutdown()
//!     .validate("/validate/myresource", ReplicaLimit)
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::body::Body;
use axum::{Json, Router, routing};
use bytes::Bytes;
use futures::future::BoxFuture;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rustls::ServerConfig;
use serde::de::DeserializeOwned;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tower::ServiceExt;
use tracing::{debug, info, warn};

use crate::error::{AdmissionError, Result};
use crate::review::{
    AdmissionRequest, ValidationResponse, build_deny_response, build_response, parse_request,
    parse_uid,
};

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

/// Builder for a validating admission webhook server.
///
/// Wraps an `axum` router and adds:
///
/// | Method | What it provides |
/// |--------|-----------------|
/// | `.port(port)` | Bind port for the webhook HTTPS/HTTP server |
/// | `.tls(config)` | TLS using a pre-built `rustls::ServerConfig` |
/// | `.tls_from_pem(cert, key)` | TLS from in-memory PEM bytes |
/// | `.health_port(port)` | `GET /healthz` + `GET /readyz` server |
/// | `.graceful_shutdown()` | Clean stop on SIGTERM or Ctrl+C |
/// | `.validate(path, validator)` | Register a typed validator at `path` |
///
/// # TLS
///
/// Kubernetes requires admission webhooks to be served over HTTPS. Configure
/// TLS by either:
///
/// - Calling `.tls(Arc::new(server_config))` with a pre-built
///   [`rustls::ServerConfig`].
/// - Calling `.tls_from_pem(&cert_pem, &key_pem)` with PEM-encoded
///   certificate and private key bytes (e.g. from a cert-manager `Secret`).
///
/// When no TLS is configured the server listens on plain HTTP, which is
/// useful when TLS is terminated by a sidecar or service mesh.
///
/// # Example
///
/// ```no_run
/// use std::fs;
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
/// let cert_pem = fs::read("/tls/tls.crt")?;
/// let key_pem  = fs::read("/tls/tls.key")?;
///
/// WebhookBuilder::new()
///     .port(8443)
///     .tls_from_pem(&cert_pem, &key_pem)?
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
    /// Defaults: port `8443`, no TLS, no health server, no graceful shutdown.
    pub fn new() -> Self {
        Self {
            port: 8443,
            tls: None,
            health_port: None,
            graceful_shutdown: false,
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
    /// (client auth, custom cipher suites, ALPN). For the common
    /// cert-manager-issued certificate case, [`tls_from_pem`][Self::tls_from_pem]
    /// is simpler.
    pub fn tls(mut self, config: Arc<ServerConfig>) -> Self {
        self.tls = Some(config);
        self
    }

    /// Configure TLS from PEM-encoded certificate and private key bytes.
    ///
    /// Accepts the raw bytes of a PEM file, as you would read from a
    /// cert-manager `Secret` volume mount (e.g. `/tls/tls.crt` and
    /// `/tls/tls.key`). Supports RSA and ECDSA private keys.
    ///
    /// Returns an error if the PEM data is malformed or contains no usable
    /// certificate/key pair.
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
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        use rustls_pemfile::{certs, private_key};

        let certs: Vec<CertificateDer<'static>> = certs(&mut &cert_pem[..])
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| AdmissionError::Tls(e.to_string()))?;

        if certs.is_empty() {
            return Err(AdmissionError::Tls(
                "no certificates found in PEM data".into(),
            ));
        }

        let key: PrivateKeyDer<'static> = private_key(&mut &key_pem[..])
            .map_err(|e| AdmissionError::Tls(e.to_string()))?
            .ok_or_else(|| AdmissionError::Tls("no private key found in PEM data".into()))?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| AdmissionError::Tls(e.to_string()))?;

        self.tls = Some(Arc::new(config));
        Ok(self)
    }

    /// Start a health probe HTTP server on `0.0.0.0:<port>`.
    ///
    /// `GET /healthz` always returns `200 OK`.
    /// `GET /readyz` returns `503` until the webhook server is bound and
    /// ready to serve requests, then `200 OK`.
    pub fn health_port(mut self, port: u16) -> Self {
        self.health_port = Some(port);
        self
    }

    /// Stop the server cleanly on SIGTERM or Ctrl+C.
    ///
    /// In-flight requests are allowed to complete. New connections are no
    /// longer accepted once the signal is received.
    pub fn graceful_shutdown(mut self) -> Self {
        self.graceful_shutdown = true;
        self
    }

    /// Register a typed [`Validator`] to handle `POST` requests at `path`.
    ///
    /// The path must match the `rules[].operations` path configured on the
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

    /// Start the webhook server.
    ///
    /// In order:
    /// 1. Binds the health server if `.health_port()` was set.
    /// 2. Binds the webhook listener on the configured port.
    /// 3. Marks `/readyz` healthy.
    /// 4. Serves requests — with TLS if configured, plain HTTP otherwise.
    /// 5. Stops cleanly on shutdown signal if `.graceful_shutdown()` was set.
    pub async fn run(self) -> Result<()> {
        let WebhookBuilder {
            port,
            tls,
            health_port,
            graceful_shutdown,
            routes,
        } = self;

        // --- Build axum router ---
        let mut router = Router::new();
        for (path, handler) in routes {
            let h = handler.clone();
            router = router.route(
                &path,
                routing::post(move |Json(body): Json<serde_json::Value>| {
                    let handler = h.clone();
                    async move { handler(body).await }
                }),
            );
        }

        // --- Health server ---
        let ready = Arc::new(AtomicBool::new(false));
        if let Some(hp) = health_port {
            let listener = TcpListener::bind(("0.0.0.0", hp)).await?;
            info!(port = hp, "Health server listening");
            tokio::spawn(serve_health(listener, ready.clone()));
        }

        // --- Shutdown signal ---
        let shutdown_rx = if graceful_shutdown {
            let (tx, rx) = tokio::sync::watch::channel(false);
            tokio::spawn(async move {
                shutdown_signal().await;
                info!("Shutdown signal received");
                tx.send(true).ok();
            });
            Some(rx)
        } else {
            None
        };

        // --- Bind webhook port ---
        let listener = TcpListener::bind(("0.0.0.0", port)).await?;
        info!(port, "Webhook server listening");
        ready.store(true, Ordering::Release);

        // --- Serve ---
        match tls {
            Some(tls_config) => {
                let acceptor = TlsAcceptor::from(tls_config);
                let serve = async {
                    loop {
                        let Ok((stream, _)) = listener.accept().await else {
                            break;
                        };
                        let acc = acceptor.clone();
                        let svc = router.clone();
                        tokio::spawn(async move {
                            let Ok(tls_stream) = acc.accept(stream).await else {
                                warn!("TLS handshake failed");
                                return;
                            };
                            let io = TokioIo::new(tls_stream);
                            let hyper_svc = service_fn(move |req: Request<Incoming>| {
                                let s = svc.clone();
                                async move { s.oneshot(req.map(Body::new)).await }
                            });
                            if let Err(e) =
                                http1::Builder::new().serve_connection(io, hyper_svc).await
                            {
                                debug!(error = %e, "Connection closed");
                            }
                        });
                    }
                    Ok::<(), AdmissionError>(())
                };
                match shutdown_rx {
                    Some(mut rx) => {
                        tokio::select! {
                            r = serve => r,
                            _ = async move { rx.changed().await.ok(); } => Ok(()),
                        }
                    }
                    None => serve.await,
                }
            }
            None => match shutdown_rx {
                Some(mut rx) => axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        rx.changed().await.ok();
                    })
                    .await
                    .map_err(AdmissionError::Io),
                None => axum::serve(listener, router)
                    .await
                    .map_err(AdmissionError::Io),
            },
        }
    }
}
