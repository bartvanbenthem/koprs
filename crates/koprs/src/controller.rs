//! Controller framework for building Kubernetes operators.
//!
//! Provides [`Reconciler`], [`Context`], and [`ControllerBuilder`] — the three
//! building blocks needed to run a reconciliation loop on top of `kube-runtime`.
//!
//! [`ControllerBuilder`] handles the operational skeleton every production
//! operator needs: graceful shutdown on SIGTERM/SIGINT, `/healthz`/`/readyz`
//! HTTP probes, leader election, and per-reconcile timeouts. Implement
//! [`Reconciler`] for your custom resource and call [`ControllerBuilder::run`]
//! — everything else is automatic.
//!
//! # Quick start
//!
//! ```no_run
//! use std::sync::Arc;
//! use koprs::controller::{Action, Context, ControllerBuilder, Reconciler};
//! use koprs::error::KubeGenericError;
//! use kube::Client;
//!
//! struct MyOperator;
//! type MyCR = k8s_openapi::api::core::v1::ConfigMap;
//!
//! impl Reconciler<MyCR> for MyOperator {
//!     type Error = KubeGenericError;
//!
//!     async fn reconcile(
//!         &self,
//!         _cr: Arc<MyCR>,
//!         _ctx: Arc<Context>,
//!     ) -> Result<Action, Self::Error> {
//!         Ok(Action::await_change())
//!     }
//! }
//!
//! # async fn example(client: Client) -> Result<(), koprs::error::KubeGenericError> {
//! let ctx = Context::new(client.clone());
//! let api = kube::Api::<MyCR>::namespaced(client, "my-namespace");
//! ControllerBuilder::new(api)
//!     .health_port(8080)
//!     .graceful_shutdown()
//!     .reconcile_timeout(std::time::Duration::from_secs(300))
//!     .run(MyOperator, ctx)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::StreamExt;
use kube::Client;
use tracing::{debug, error, info, warn};

use crate::error::{KubeGenericError, Result};
use crate::leader::{LeaderElectionConfig, LeaderElector};
use crate::observability::{Metrics, serve_metrics};
use crate::traits::KubeResource;

/// Upper bound on how long [`ControllerBuilder::run`] waits to release the
/// leader lease during shutdown.
const LEASE_RELEASE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Re-exports — operators only need `use koprs::controller::*`
// ---------------------------------------------------------------------------

pub use kube_runtime::controller::Action;
pub use kube_runtime::reflector::ObjectRef;
pub use kube_runtime::watcher;

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Shared context passed to every reconcile and error-policy call.
///
/// Holds the Kubernetes [`Client`] and any user-supplied data `T`.
/// Use [`Context::new`] when no extra state is needed, or
/// [`Context::with_data`] to attach your own configuration, metrics
/// handles, or other shared state.
///
/// The `T = ()` default means [`Context`] is a valid type alias for
/// simple operators that do not need extra data.
pub struct Context<T = ()> {
    /// The Kubernetes API client.
    pub client: Client,
    /// User-supplied data shared across all reconcile calls.
    pub data: T,
}

impl Context<()> {
    /// Create a context with a Kubernetes client and no extra data.
    pub fn new(client: Client) -> Arc<Self> {
        Arc::new(Self { client, data: () })
    }
}

impl<T> Context<T> {
    /// Create a context with a Kubernetes client and user-supplied data.
    pub fn with_data(client: Client, data: T) -> Arc<Self> {
        Arc::new(Self { client, data })
    }
}

// ---------------------------------------------------------------------------
// Reconciler trait
// ---------------------------------------------------------------------------

/// Trait that operator authors implement to define reconciliation logic.
///
/// `CR` is the primary custom resource type being reconciled.
/// `T` is the shared context data type and defaults to `()`.
///
/// `error_policy` has a default implementation that requeues after 30 seconds.
/// Override it only if you need custom backoff or logging.
///
/// # Implementing
///
/// ```no_run
/// use std::sync::Arc;
/// use koprs::controller::{Action, Context, Reconciler};
/// use koprs::error::KubeGenericError;
///
/// struct MyOperator;
/// type MyCR = k8s_openapi::api::core::v1::ConfigMap;
///
/// impl Reconciler<MyCR> for MyOperator {
///     type Error = KubeGenericError;
///
///     async fn reconcile(
///         &self,
///         _cr: Arc<MyCR>,
///         _ctx: Arc<Context>,
///     ) -> Result<Action, Self::Error> {
///         Ok(Action::await_change())
///     }
///     // error_policy defaults to requeue(30s) — no need to implement it
/// }
/// ```
pub trait Reconciler<CR, T = ()>: Send + Sync + 'static
where
    CR: KubeResource,
    T: Send + Sync + 'static,
{
    /// The error type returned by [`reconcile`](Self::reconcile).
    type Error: std::error::Error + Send + Sync + 'static;

    /// Reconcile the given resource to its desired state.
    ///
    /// Called whenever the resource changes or a requeue fires. Return
    /// [`Action::await_change`] to wait for the next watch event or
    /// [`Action::requeue`] to retry after a fixed duration.
    ///
    /// The returned future must be `Send` because the controller runtime
    /// executes reconciles on a multi-threaded executor.
    fn reconcile(
        &self,
        cr: Arc<CR>,
        ctx: Arc<Context<T>>,
    ) -> impl std::future::Future<Output = std::result::Result<Action, Self::Error>> + Send;

    /// Decide how to handle a failed reconcile.
    ///
    /// Defaults to requeue after 30 seconds. Override to implement custom
    /// backoff strategies or to add per-error logging.
    fn error_policy(&self, _cr: Arc<CR>, _err: &Self::Error, _ctx: Arc<Context<T>>) -> Action {
        Action::requeue(Duration::from_secs(30))
    }

    /// Map an error to the `error` label of the `reconcile_errors_total` metric.
    ///
    /// The returned value **must come from a small, fixed set** — never embed
    /// object names, messages, or other request data, or the metric's
    /// cardinality grows without bound. Defaults to `"reconcile_error"`.
    /// When your error type is [`KubeGenericError`], return
    /// [`KubeGenericError::metric_label`].
    fn error_label(&self, _err: &Self::Error) -> &'static str {
        "reconcile_error"
    }
}

// ---------------------------------------------------------------------------
// Internal: health server
// ---------------------------------------------------------------------------

/// Serve HTTP/1.1 health probes on an already-bound listener.
///
/// `GET /healthz` — `200 OK` always (liveness).
/// `GET /readyz`  — `200 OK` once `ready` flips to `true`, else `503` (readiness).
pub(crate) async fn serve_health(listener: tokio::net::TcpListener, ready: Arc<AtomicBool>) {
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;

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

/// Resolves once the shutdown flag flips to `true`. Never resolves if the
/// sender is dropped without signalling.
async fn wait_for_shutdown(mut rx: tokio::sync::watch::Receiver<bool>) {
    if rx.wait_for(|stop| *stop).await.is_err() {
        std::future::pending::<()>().await;
    }
}

/// Resolves on SIGTERM (Unix) or Ctrl+C (all platforms).
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
// ControllerBuilder
// ---------------------------------------------------------------------------

type ConfigureFn<CR> =
    Box<dyn FnOnce(kube_runtime::Controller<CR>) -> kube_runtime::Controller<CR> + Send + 'static>;

/// Builder for a controller reconciliation loop.
///
/// Wraps `kube-runtime::Controller` and adds:
///
/// | Method | What it provides |
/// |--------|-----------------|
/// | `.health_port(port)` | `GET /healthz` + `GET /readyz` server |
/// | `.graceful_shutdown()` | Clean stop on SIGTERM or Ctrl+C |
/// | `.leader_election(ns, name)` | Kubernetes Lease-based HA leader election |
/// | `.reconcile_timeout(dur)` | Kill and requeue stuck reconciles |
///
/// # Example
///
/// ```no_run
/// use koprs::controller::{Action, Context, ControllerBuilder, ObjectRef, Reconciler, watcher};
/// use koprs::error::KubeGenericError;
/// use kube::{Api, Client};
/// use k8s_openapi::api::core::v1::ConfigMap;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// struct MyOperator;
/// type MyCR = k8s_openapi::api::core::v1::ConfigMap;
///
/// impl Reconciler<MyCR> for MyOperator {
///     type Error = KubeGenericError;
///     async fn reconcile(&self, _cr: Arc<MyCR>, _ctx: Arc<Context>) -> Result<Action, KubeGenericError> {
///         Ok(Action::await_change())
///     }
/// }
///
/// # async fn example(client: Client, cm_api: Api<ConfigMap>) -> Result<(), KubeGenericError> {
/// let ctx = Context::new(client.clone());
/// let api = Api::<MyCR>::namespaced(client, "my-namespace");
/// ControllerBuilder::new(api)
///     .health_port(8080)
///     .graceful_shutdown()
///     .leader_election("my-namespace", "my-operator-leader")
///     .reconcile_timeout(Duration::from_secs(300))
///     .run(MyOperator, ctx)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ControllerBuilder<CR, T = ()>
where
    CR: KubeResource,
{
    api: kube::Api<CR>,
    pub(crate) watcher_config: watcher::Config,
    pub(crate) configure: Option<ConfigureFn<CR>>,
    pub(crate) health_port: Option<u16>,
    pub(crate) metrics_port: Option<u16>,
    pub(crate) graceful_shutdown: bool,
    pub(crate) reconcile_timeout: Option<Duration>,
    pub(crate) leader_election: Option<LeaderElectionConfig>,
    pub(crate) leader_election_timings: Option<LeaderElectionTimings>,
    pub(crate) leader_election_identity: Option<String>,
    pub(crate) concurrency: Option<u16>,
    _phantom: PhantomData<T>,
}

/// Timings supplied through [`ControllerBuilder::leader_election_timings`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeaderElectionTimings {
    pub(crate) lease_duration: Duration,
    pub(crate) renew_deadline: Duration,
    pub(crate) retry_period: Duration,
}

impl<CR, T> ControllerBuilder<CR, T>
where
    CR: KubeResource,
    T: Send + Sync + 'static,
{
    /// Create a new builder from a pre-built [`kube::Api`].
    ///
    /// The `Api` determines the watch scope:
    /// - `Api::namespaced(client, "ns")` — one namespace
    /// - `Api::all(client)` — all namespaces / cluster-wide
    pub fn new(api: kube::Api<CR>) -> Self {
        Self {
            api,
            watcher_config: watcher::Config::default(),
            configure: None,
            health_port: None,
            metrics_port: None,
            graceful_shutdown: false,
            reconcile_timeout: None,
            leader_election: None,
            leader_election_timings: None,
            leader_election_identity: None,
            concurrency: None,
            _phantom: PhantomData,
        }
    }

    /// Start a health probe HTTP server on `0.0.0.0:<port>`.
    ///
    /// `GET /healthz` always returns `200 OK` (liveness).
    /// `GET /readyz` returns `200 OK` once the operator has finished starting
    /// up (servers bound, configuration validated) and `503` again after a
    /// shutdown signal. Readiness does **not** depend on leadership or on a
    /// first reconcile, so standby replicas and operators with zero CRs are
    /// Ready and Deployment rollouts are never blocked.
    ///
    /// If the port is already in use, [`run`](ControllerBuilder::run) returns
    /// an error before the controller loop starts.
    pub fn health_port(mut self, port: u16) -> Self {
        self.health_port = Some(port);
        self
    }

    /// Start a Prometheus metrics server on `0.0.0.0:<port>`.
    ///
    /// `GET /metrics` returns reconcile counts, error counts (by kind and
    /// [error label](Reconciler::error_label)), and reconcile duration
    /// histograms in Prometheus text exposition format. Recording happens
    /// automatically around every reconcile — see [`Metrics`] for the full
    /// list of collectors.
    ///
    /// If the port is already in use, [`run`](ControllerBuilder::run) returns
    /// an error before the controller loop starts.
    pub fn metrics_port(mut self, port: u16) -> Self {
        self.metrics_port = Some(port);
        self
    }

    /// Stop the controller cleanly on SIGTERM or Ctrl+C.
    ///
    /// On the signal, `/readyz` switches to `503`, the controller stops
    /// starting new reconciles, and reconciles already in flight are allowed
    /// to finish (bound them with [`reconcile_timeout`](Self::reconcile_timeout)).
    /// With leader election enabled the lease is then released so a standby
    /// takes over immediately. A replica still waiting for leadership returns
    /// straight away.
    pub fn graceful_shutdown(mut self) -> Self {
        self.graceful_shutdown = true;
        self
    }

    /// Enable Kubernetes Lease-based leader election.
    ///
    /// [`run`](ControllerBuilder::run) waits until this pod acquires the named
    /// `Lease` in `namespace`, then starts the controller. Defaults follow
    /// client-go: lease duration 15s, renew deadline 10s, retry period 2s
    /// (see [`leader_election_timings`](Self::leader_election_timings)).
    ///
    /// Expiry is judged with the local monotonic clock, so clock skew between
    /// nodes cannot produce two leaders. If the lease cannot be renewed within
    /// the renew deadline, or another replica takes it, the controller is
    /// stopped **immediately** (in-flight reconciles are cancelled) and `run`
    /// returns [`KubeGenericError::LeaderElectionLost`]. Exit the process with
    /// a non-zero status on that error so Kubernetes restarts the pod and it
    /// campaigns again.
    ///
    /// The pod's identity defaults to `$POD_NAME` (downward API) falling back
    /// to `$HOSTNAME`. If neither is set, `run` returns a configuration error
    /// rather than risk two replicas sharing an identity.
    pub fn leader_election(
        mut self,
        namespace: impl Into<String>,
        name: impl Into<String>,
    ) -> Self {
        let identity = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_default();
        self.leader_election = Some(LeaderElectionConfig::new(
            namespace.into(),
            name.into(),
            identity,
        ));
        self
    }

    /// Cancel and requeue any reconcile that runs longer than `timeout`.
    ///
    /// The resource is requeued after the same duration and the attempt is
    /// counted as a failure with error label `"timeout"` in the metrics.
    pub fn reconcile_timeout(mut self, timeout: Duration) -> Self {
        self.reconcile_timeout = Some(timeout);
        self
    }

    /// Apply a label selector to the primary resource watch.
    pub fn label_selector(mut self, selector: impl Into<String>) -> Self {
        let s = selector.into();
        self.watcher_config = self.watcher_config.labels(&s);
        self
    }

    /// Override the watcher configuration for the primary resource watch.
    pub fn watcher_config(mut self, config: watcher::Config) -> Self {
        self.watcher_config = config;
        self
    }

    /// Limit the number of reconciles that may run concurrently.
    ///
    /// Unbounded by default. Set to a positive value to cap concurrent
    /// reconciles across all objects; `0` means unbounded. A single object is
    /// never reconciled concurrently regardless of this setting.
    pub fn concurrency(mut self, n: u16) -> Self {
        self.concurrency = Some(n);
        self
    }

    /// Override the timing parameters for leader election.
    ///
    /// May be called before or after
    /// [`.leader_election()`](ControllerBuilder::leader_election). The
    /// semantics match client-go:
    ///
    /// | Parameter | Meaning | Default |
    /// |-----------|---------|---------|
    /// | `lease_duration` | How long candidates wait, after last seeing the lease change, before taking it over | 15s |
    /// | `renew_deadline` | How long the leader keeps retrying a renewal before it steps down | 10s |
    /// | `retry_period` | Interval between acquire / renew attempts | 2s |
    ///
    /// [`run`](ControllerBuilder::run) returns
    /// [`KubeGenericError::Config`] unless
    /// `lease_duration > renew_deadline > retry_period > 0`, or if this is set
    /// without `.leader_election()`.
    pub fn leader_election_timings(
        mut self,
        lease_duration: Duration,
        renew_deadline: Duration,
        retry_period: Duration,
    ) -> Self {
        self.leader_election_timings = Some(LeaderElectionTimings {
            lease_duration,
            renew_deadline,
            retry_period,
        });
        self
    }

    /// Override the leader-election identity written to the Lease.
    ///
    /// Defaults to `$POD_NAME`, then `$HOSTNAME`. Every replica must use a
    /// distinct identity. May be called before or after `.leader_election()`.
    pub fn leader_election_identity(mut self, identity: impl Into<String>) -> Self {
        self.leader_election_identity = Some(identity.into());
        self
    }

    /// Combine `.leader_election()` and `.leader_election_timings()` into a
    /// validated configuration.
    pub(crate) fn resolve_leader_election(&self) -> Result<Option<LeaderElectionConfig>> {
        match (&self.leader_election, self.leader_election_timings) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(KubeGenericError::Config(
                "leader_election_timings() was set without leader_election()".into(),
            )),
            (Some(cfg), timings) => {
                let mut cfg = cfg.clone();
                if let Some(identity) = &self.leader_election_identity {
                    cfg.identity.clone_from(identity);
                }
                if let Some(t) = timings {
                    cfg.lease_duration = t.lease_duration;
                    cfg.renew_deadline = t.renew_deadline;
                    cfg.retry_period = t.retry_period;
                }
                if cfg.identity.is_empty() {
                    return Err(KubeGenericError::Config(
                        "leader election needs a unique identity: set POD_NAME \
                         (downward API) or HOSTNAME"
                            .into(),
                    ));
                }
                cfg.validate()?;
                Ok(Some(cfg))
            }
        }
    }

    /// Watch child resources that this CR owns via Kubernetes owner references.
    ///
    /// Whenever a resource of type `Child` changes, `kube-runtime` traverses
    /// its owner references to find the parent CR and re-queues it. Use this
    /// for resources you create and set an owner reference on (e.g. Deployments,
    /// Services, ConfigMaps managed by your operator).
    ///
    /// The `Child` type must carry an [`OwnerReference`] pointing back at `CR`.
    ///
    /// Multiple calls compose — each call adds another owned type on top of
    /// previously registered watches.
    ///
    /// [`OwnerReference`]: k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference
    ///
    /// # Example
    ///
    /// ```no_run
    /// use k8s_openapi::api::apps::v1::Deployment;
    /// use k8s_openapi::api::core::v1::ConfigMap;
    /// use koprs::controller::{ControllerBuilder, watcher};
    ///
    /// async fn example(api: kube::Api<ConfigMap>, deploy_api: kube::Api<Deployment>) {
    ///     let _builder: ControllerBuilder<ConfigMap> = ControllerBuilder::new(api)
    ///         .owns(deploy_api, watcher::Config::default());
    /// }
    /// ```
    pub fn owns<Child>(mut self, api: kube::Api<Child>, config: watcher::Config) -> Self
    where
        Child: crate::traits::KubeResource,
    {
        let existing = self.configure.take();
        self.configure = Some(Box::new(move |ctl| {
            let ctl = ctl.owns(api, config);
            match existing {
                Some(f) => f(ctl),
                None => ctl,
            }
        }));
        self
    }

    /// Wire a single secondary (trigger) watch using a typed mapper.
    ///
    /// Whenever a resource of type `Other` changes, `mapper` converts it into
    /// zero or more [`ObjectRef`]s that identify CRs to re-queue. Returning
    /// `None` / an empty iterator drops the event. Returning
    /// `Some(ObjectRef::new("my-cr").within("my-ns"))` re-queues that CR.
    ///
    /// Multiple calls to `.watch()` compose — each watch is added on top of
    /// any previously registered watches and `.with_watches()` calls.
    ///
    /// Use [`owner_label_mapper`][crate::owners::owner_label_mapper] as the
    /// `mapper` argument when the trigger resource carries an owner label
    /// pointing at the CR.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
    /// use koprs::controller::{ControllerBuilder, watcher};
    /// use koprs::owners::owner_label_mapper;
    ///
    /// async fn example(api: kube::Api<Namespace>, cm_api: kube::Api<ConfigMap>) {
    ///     // Re-queue the owning Namespace whenever a managed ConfigMap changes.
    ///     let _builder: ControllerBuilder<Namespace> = ControllerBuilder::new(api)
    ///         .watch(
    ///             cm_api,
    ///             watcher::Config::default().labels("app=my-operator"),
    ///             owner_label_mapper::<ConfigMap, Namespace>("my-operator/owner"),
    ///         );
    ///     // chain .health_port(), .leader_election(), .run(), etc.
    /// }
    /// ```
    pub fn watch<Other, I, F>(
        mut self,
        api: kube::Api<Other>,
        config: watcher::Config,
        mapper: F,
    ) -> Self
    where
        Other: crate::traits::KubeResource,
        I: IntoIterator<Item = kube_runtime::reflector::ObjectRef<CR>> + Send + 'static,
        I::IntoIter: Send,
        F: Fn(Other) -> I + Send + Sync + 'static,
    {
        let existing = self.configure.take();
        self.configure = Some(Box::new(move |ctl| {
            let ctl = ctl.watches(api, config, mapper);
            match existing {
                Some(f) => f(ctl),
                None => ctl,
            }
        }));
        self
    }

    /// Configure additional watches using the full `kube_runtime::Controller` API.
    ///
    /// The closure receives the inner controller and must return it, typically
    /// with `.watches()` or `.owns()` chained. Use this when you need access to
    /// advanced kube-runtime options not covered by [`watch`][ControllerBuilder::watch].
    ///
    /// Multiple calls compose — each call adds on top of previously registered
    /// watches.
    pub fn with_watches<F>(mut self, configure: F) -> Self
    where
        F: FnOnce(kube_runtime::Controller<CR>) -> kube_runtime::Controller<CR> + Send + 'static,
    {
        let existing = self.configure.take();
        self.configure = Some(Box::new(move |ctl| {
            let ctl = configure(ctl);
            match existing {
                Some(f) => f(ctl),
                None => ctl,
            }
        }));
        self
    }

    /// Start the reconciliation loop.
    ///
    /// In order:
    /// 1. Validates the configuration and binds the health and metrics servers.
    /// 2. Marks `/readyz` ready.
    /// 3. Waits for the leader lease if `.leader_election()` was set (returns
    ///    `Ok(())` if a shutdown signal arrives first).
    /// 4. Runs the reconcile loop, wrapping each call with a timeout if set.
    /// 5. On a shutdown signal, lets in-flight reconciles finish, then
    ///    releases the lease and returns `Ok(())`.
    ///
    /// # Errors
    ///
    /// Returns [`KubeGenericError::LeaderElectionLost`] if leadership is lost
    /// while running. The controller has already been stopped; exit the
    /// process so the pod restarts and campaigns again.
    pub async fn run<R>(self, reconciler: R, context: Arc<Context<T>>) -> Result<()>
    where
        R: Reconciler<CR, T>,
    {
        let kind = CR::kind(&()).to_string();
        let client = context.client.clone();
        let leader_config = self.resolve_leader_election()?;
        info!(%kind, "Starting controller");

        // --- Health probes ---
        let ready = Arc::new(AtomicBool::new(false));
        if let Some(port) = self.health_port {
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
            info!(port, "Health server listening");
            tokio::spawn(serve_health(listener, ready.clone()));
        }

        // --- Metrics ---
        let metrics = if let Some(port) = self.metrics_port {
            let registry = prometheus::Registry::new();
            let metrics = Arc::new(Metrics::new_registered("", &registry)?);
            let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
            info!(port, "Metrics server listening");
            tokio::spawn(serve_metrics(listener, registry));
            Some(metrics)
        } else {
            None
        };

        // --- Shutdown signal ---
        // `_shutdown_tx` is kept alive for the whole function so receivers
        // never observe a closed channel.
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        if self.graceful_shutdown {
            let tx = _shutdown_tx.clone();
            let ready = ready.clone();
            let kind = kind.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                info!(%kind, "Shutdown signal received");
                ready.store(false, Ordering::Release);
                tx.send(true).ok();
            });
        }

        // Start-up is complete: report ready. Standby replicas are ready too,
        // so rolling updates are not blocked by leader election.
        ready.store(true, Ordering::Release);

        // --- Leader election: wait for the lease ---
        let mut elector = leader_config.map(|cfg| LeaderElector::new(cfg, client.clone()));
        if let Some(el) = elector.as_mut() {
            tokio::select! {
                () = el.acquire() => {}
                () = wait_for_shutdown(shutdown_rx.clone()) => {
                    info!(%kind, "Shutdown requested before leadership was acquired");
                    return Ok(());
                }
            }
        }

        // --- Controller ---
        let mut ctl = kube_runtime::Controller::new(self.api, self.watcher_config);
        if let Some(n) = self.concurrency {
            ctl = ctl.with_config(kube_runtime::controller::Config::default().concurrency(n));
        }
        if let Some(configure) = self.configure {
            ctl = configure(ctl);
        }
        if self.graceful_shutdown {
            // kube-runtime stops scheduling new work and drains in-flight
            // reconciles before the stream ends.
            ctl = ctl.graceful_shutdown_on(wait_for_shutdown(shutdown_rx.clone()));
        }

        let reconciler = Arc::new(reconciler);
        let error_policy_r = reconciler.clone();
        let reconcile_timeout = self.reconcile_timeout;
        let kind_label = kind.clone();

        let run_loop = ctl
            .run(
                move |cr, ctx| {
                    let r = reconciler.clone();
                    let metrics = metrics.clone();
                    let kind = kind_label.clone();
                    async move {
                        let started = std::time::Instant::now();
                        let outcome = match reconcile_timeout {
                            Some(t) => tokio::time::timeout(t, r.reconcile(cr, ctx))
                                .await
                                .map_err(|_| t),
                            None => Ok(r.reconcile(cr, ctx).await),
                        };
                        let elapsed = started.elapsed();
                        match outcome {
                            Ok(result) => {
                                if let Some(m) = &metrics {
                                    match &result {
                                        Ok(_) => m.record_success(&kind, elapsed),
                                        Err(e) => {
                                            m.record_failure(&kind, r.error_label(e), elapsed)
                                        }
                                    }
                                }
                                result
                            }
                            Err(t) => {
                                warn!(%kind, timeout = ?t, "Reconcile timed out, requeueing");
                                if let Some(m) = &metrics {
                                    m.record_failure(&kind, "timeout", elapsed);
                                }
                                Ok(Action::requeue(t))
                            }
                        }
                    }
                },
                move |cr, err, ctx| error_policy_r.error_policy(cr, err, ctx),
                context,
            )
            .for_each(|result| async move {
                match result {
                    Ok((obj, _)) => debug!(name = %obj.name, "Reconcile succeeded"),
                    Err(e) => warn!(error = %e, "Reconcile failed"),
                }
            });

        let outcome = match elector.as_mut() {
            Some(el) => {
                tokio::select! {
                    () = run_loop => Ok(()),
                    lost = el.renew_until_lost() => {
                        // Dropping `run_loop` cancels in-flight reconciles: we
                        // are no longer the leader and must not keep writing.
                        error!(%kind, error = %lost, "Stopping controller");
                        Err(lost)
                    }
                }
            }
            None => {
                run_loop.await;
                Ok(())
            }
        };

        if outcome.is_ok()
            && let Some(el) = elector.as_mut()
        {
            match tokio::time::timeout(LEASE_RELEASE_TIMEOUT, el.release()).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(error = %e, "Failed to release leader lease"),
                Err(_) => warn!("Timed out releasing leader lease"),
            }
        }

        info!(%kind, "Controller stopped");
        outcome
    }
}
