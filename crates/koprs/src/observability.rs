//! Operator observability — Prometheus metrics for the reconciliation loop.
//!
//! Provides [`Metrics`], a small set of Prometheus collectors that track the
//! three numbers every operator dashboard needs: how many reconciles ran, how
//! many failed (and why), and how long they took.
//! [`ControllerBuilder`][crate::controller::ControllerBuilder] wires this in
//! automatically when `.metrics_port()` is set — recording around every reconcile and serving the result on
//! `GET /metrics` in Prometheus text-exposition format.
//!
//! # Quick start
//!
//! ```no_run
//! use koprs::controller::{Action, Context, ControllerBuilder, Reconciler};
//! use koprs::error::KubeGenericError;
//! use kube::Client;
//! use std::sync::Arc;
//!
//! struct MyOperator;
//! type MyCR = k8s_openapi::api::core::v1::ConfigMap;
//!
//! impl Reconciler<MyCR> for MyOperator {
//!     type Error = KubeGenericError;
//!     async fn reconcile(&self, _cr: Arc<MyCR>, _ctx: Arc<Context>) -> Result<Action, KubeGenericError> {
//!         Ok(Action::await_change())
//!     }
//! }
//!
//! # async fn example(client: Client) -> Result<(), KubeGenericError> {
//! let ctx = Context::new(client.clone());
//! let api = kube::Api::<MyCR>::namespaced(client, "my-namespace");
//! ControllerBuilder::new(api)
//!     .metrics_port(9090)
//!     .run(MyOperator, ctx)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::time::Duration;

use prometheus::{Encoder, HistogramVec, IntCounter, IntCounterVec, Opts, Registry, TextEncoder};

use crate::error::{KubeGenericError, Result};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Prometheus collectors for the reconciliation loop.
///
/// | Metric | Type | Labels | Meaning |
/// |--------|------|--------|---------|
/// | `koprs_reconciliations_total` | counter | — | Reconciles completed, success or failure |
/// | `koprs_reconcile_errors_total` | counter | `kind`, `error` | Failed reconciles, by resource kind and error |
/// | `koprs_reconcile_duration_seconds` | histogram | `kind` | Reconcile latency, by resource kind |
///
/// Construct with [`Metrics::new`] and register with a [`Registry`] via
/// [`Metrics::register`] — or use [`Metrics::new_registered`] to do both at
/// once. [`ControllerBuilder`][crate::controller::ControllerBuilder] does this
/// for you when `.metrics_port()` is set.
#[derive(Clone, Debug)]
pub struct Metrics {
    reconciliations: IntCounter,
    errors: IntCounterVec,
    reconcile_duration: HistogramVec,
}

impl Metrics {
    /// Create the collectors without registering them.
    pub fn new() -> Self {
        let reconciliations = IntCounter::new(
            "koprs_reconciliations_total",
            "Total number of reconciles completed",
        )
        .expect("static metric options are valid");

        let errors = IntCounterVec::new(
            Opts::new(
                "koprs_reconcile_errors_total",
                "Total number of failed reconciles",
            ),
            &["kind", "error"],
        )
        .expect("static metric options are valid");

        let reconcile_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "koprs_reconcile_duration_seconds",
                "Reconcile latency in seconds",
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 15.0, 60.0]),
            &["kind"],
        )
        .expect("static metric options are valid");

        Self {
            reconciliations,
            errors,
            reconcile_duration,
        }
    }

    /// Register all collectors with `registry`.
    ///
    /// Returns [`KubeGenericError::Internal`] if a collector with the same
    /// name is already registered.
    pub fn register(self, registry: &Registry) -> Result<Self> {
        registry
            .register(Box::new(self.reconciliations.clone()))
            .map_err(|e| KubeGenericError::Internal(format!("failed to register metrics: {e}")))?;
        registry
            .register(Box::new(self.errors.clone()))
            .map_err(|e| KubeGenericError::Internal(format!("failed to register metrics: {e}")))?;
        registry
            .register(Box::new(self.reconcile_duration.clone()))
            .map_err(|e| KubeGenericError::Internal(format!("failed to register metrics: {e}")))?;
        Ok(self)
    }

    /// Create the collectors and register them with `registry` in one step.
    pub fn new_registered(registry: &Registry) -> Result<Self> {
        Self::new().register(registry)
    }

    /// Record a successful reconcile of `kind` that took `duration`.
    pub fn record_success(&self, kind: &str, duration: Duration) {
        self.reconciliations.inc();
        self.reconcile_duration
            .with_label_values(&[kind])
            .observe(duration.as_secs_f64());
    }

    /// Record a failed reconcile of `kind` that took `duration`, labelled
    /// with the error's `Display` representation.
    pub fn record_failure(&self, kind: &str, error: &str, duration: Duration) {
        self.reconciliations.inc();
        self.errors.with_label_values(&[kind, error]).inc();
        self.reconcile_duration
            .with_label_values(&[kind])
            .observe(duration.as_secs_f64());
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Internal: metrics server
// ---------------------------------------------------------------------------

/// Render every metric family in `registry` as Prometheus text exposition format.
pub(crate) fn render(registry: &Registry) -> Result<String> {
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    TextEncoder::new()
        .encode(&metric_families, &mut buffer)
        .map_err(|e| KubeGenericError::Internal(format!("failed to encode metrics: {e}")))?;
    String::from_utf8(buffer)
        .map_err(|e| KubeGenericError::Internal(format!("metrics output was not valid UTF-8: {e}")))
}

/// Serve `GET /metrics` on an already-bound listener, rendering `registry`
/// in Prometheus text exposition format on every request.
pub(crate) async fn serve_metrics(listener: tokio::net::TcpListener, registry: Registry) {
    use axum::Router;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::get;

    async fn metrics_handler(State(registry): State<Registry>) -> (StatusCode, String) {
        match render(&registry) {
            Ok(body) => (StatusCode::OK, body),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry);

    let _ = axum::serve(listener, app).await;
}
