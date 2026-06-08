// src/main.rs
//
// Demonstrates running multiple independent controllers — each managing its
// own CRD kind — concurrently inside a single operator process.
//
//   SecretSync         controller — reconciles SecretSync CRs, manages Secrets
//   ServiceAccountSync controller — reconciles ServiceAccountSync CRs, manages ServiceAccounts
//
// Each `ControllerBuilder::run(...)` call returns a future that drives its own
// watch + reconcile loop. Running them with `tokio::try_join!` polls both
// loops on the same runtime: CRs of either kind are picked up and reconciled
// in parallel, and within each loop multiple CRs of that kind are reconciled
// concurrently up to `.concurrency(n)`.
//
// Operational features (composed identically on both controllers):
//   .health_port(...)      — GET /healthz + GET /readyz per controller (distinct ports)
//   .graceful_shutdown()   — clean stop on SIGTERM / Ctrl+C
//   .leader_election(...)  — Kubernetes Lease-based HA (distinct lease names)
//   .reconcile_timeout(...) — kills and requeues reconciles stuck too long
//   .concurrency(n)        — reconcile up to n CRs of that kind in parallel

mod secretsync;
mod serviceaccountsync;
mod types;

use std::time::Duration;

use kube::{Api, Client};
use tracing::info;

use koprs::controller::{Context, ControllerBuilder};

use crate::secretsync::SecretSyncReconciler;
use crate::serviceaccountsync::ServiceAccountSyncReconciler;
use crate::types::{SecretSync, ServiceAccountSync};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    info!("starting multicontroller-operator");

    let client = Client::try_default().await?;

    // The operator namespace is injected via the downward API in production:
    //   env:
    //     - name: OPERATOR_NAMESPACE
    //       valueFrom:
    //         fieldRef:
    //           fieldPath: metadata.namespace
    let operator_ns = std::env::var("OPERATOR_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    // -----------------------------------------------------------------------
    // Controller A — SecretSync
    // -----------------------------------------------------------------------
    let secretsync_api: Api<SecretSync> = Api::all(client.clone());
    let secret_ctx = Context::new(client.clone());

    let secretsync_controller = ControllerBuilder::new(secretsync_api)
        .health_port(8080)
        .graceful_shutdown()
        .leader_election(operator_ns.clone(), "secretsync-operator-leader")
        .reconcile_timeout(Duration::from_secs(300))
        .concurrency(4)
        .run(SecretSyncReconciler, secret_ctx);

    // -----------------------------------------------------------------------
    // Controller B — ServiceAccountSync
    // -----------------------------------------------------------------------
    let serviceaccountsync_api: Api<ServiceAccountSync> = Api::all(client.clone());
    let serviceaccount_ctx = Context::new(client.clone());

    let serviceaccountsync_controller = ControllerBuilder::new(serviceaccountsync_api)
        .health_port(8081)
        .graceful_shutdown()
        .leader_election(operator_ns, "serviceaccountsync-operator-leader")
        .reconcile_timeout(Duration::from_secs(300))
        .concurrency(4)
        .run(ServiceAccountSyncReconciler, serviceaccount_ctx);

    // Drive both controller loops on the same runtime: each polls its own
    // watch stream and reconciles its own CRs independently and in parallel.
    // If either returns an error, the other is cancelled and the error is
    // propagated — mirroring how a single `.run()` call would fail the process.
    let ((), ()) = tokio::try_join!(secretsync_controller, serviceaccountsync_controller)?;

    Ok(())
}
