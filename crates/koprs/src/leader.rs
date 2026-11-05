//! Lease-based leader election used by [`ControllerBuilder`](crate::controller::ControllerBuilder).
//!
//! The algorithm follows client-go's `leaderelection` package:
//!
//! * **Expiry is judged with the local monotonic clock**, never by comparing
//!   our wall clock to the `renewTime` another node wrote. A candidate records
//!   *when it observed* the lease's `resourceVersion` change and treats the
//!   lease as expired only after `leaseDurationSeconds` have passed since that
//!   observation without a further change. Clock skew between nodes therefore
//!   cannot cause two leaders.
//! * **The leader steps down before anybody can take over.** It retries the
//!   renewal every `retry_period` and gives up once `renew_deadline` has
//!   elapsed since the start of its last successful renewal. Because
//!   `renew_deadline < lease_duration`, the old leader has stopped before any
//!   candidate considers the lease expired.
//! * **Every API call is bounded** by the remaining renew deadline, so a hung
//!   request cannot keep a stale leader running.
//! * **The lease is released on graceful shutdown** (holder cleared,
//!   duration set to 1s) so a standby takes over immediately instead of
//!   waiting for the full lease duration.

use std::time::{Duration, Instant};

use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::Client;
use kube::api::{Api, PostParams};
use tracing::{debug, info, warn};

use crate::error::{KubeGenericError, Result};

/// Default `leaseDurationSeconds` written to the Lease.
pub(crate) const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
/// Default time the leader keeps retrying a renewal before stepping down.
pub(crate) const DEFAULT_RENEW_DEADLINE: Duration = Duration::from_secs(10);
/// Default interval between acquire / renew attempts.
pub(crate) const DEFAULT_RETRY_PERIOD: Duration = Duration::from_secs(2);

/// Leader-election settings.
#[derive(Debug, Clone)]
pub(crate) struct LeaderElectionConfig {
    pub(crate) namespace: String,
    pub(crate) name: String,
    pub(crate) identity: String,
    /// How long a lease is valid without renewal, as seen by candidates.
    pub(crate) lease_duration: Duration,
    /// How long the leader keeps retrying a renewal before it steps down.
    pub(crate) renew_deadline: Duration,
    /// Interval between acquire / renew attempts.
    pub(crate) retry_period: Duration,
}

impl LeaderElectionConfig {
    pub(crate) fn new(namespace: String, name: String, identity: String) -> Self {
        Self {
            namespace,
            name,
            identity,
            lease_duration: DEFAULT_LEASE_DURATION,
            renew_deadline: DEFAULT_RENEW_DEADLINE,
            retry_period: DEFAULT_RETRY_PERIOD,
        }
    }

    /// Reject timings that would allow two leaders at once.
    pub(crate) fn validate(&self) -> Result<()> {
        let err = |msg: String| Err(KubeGenericError::Config(msg));
        if self.identity.is_empty() {
            return err("leader election identity must not be empty".into());
        }
        if self.retry_period.is_zero() {
            return err("leader election retry_period must be greater than zero".into());
        }
        if self.lease_duration < Duration::from_secs(1) {
            return err("leader election lease_duration must be at least 1s".into());
        }
        if self.lease_duration.as_secs() > i32::MAX as u64 {
            return err("leader election lease_duration is too large".into());
        }
        if self.renew_deadline >= self.lease_duration {
            return err(format!(
                "leader election renew_deadline ({:?}) must be shorter than lease_duration ({:?})",
                self.renew_deadline, self.lease_duration
            ));
        }
        if self.retry_period >= self.renew_deadline {
            return err(format!(
                "leader election retry_period ({:?}) must be shorter than renew_deadline ({:?})",
                self.retry_period, self.renew_deadline
            ));
        }
        Ok(())
    }

    /// `leaseDurationSeconds`, rounded **up** so the written duration is never
    /// shorter than the configured one.
    pub(crate) fn lease_duration_secs(&self) -> i32 {
        let secs =
            self.lease_duration.as_secs() + u64::from(self.lease_duration.subsec_nanos() > 0);
        i32::try_from(secs).unwrap_or(i32::MAX)
    }
}

/// Result of a single acquire / renew attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Attempt {
    /// We hold the lease (newly acquired or renewed).
    Held,
    /// Another identity holds a lease that has not expired from our point of view.
    HeldByOther(String),
    /// Optimistic-concurrency conflict (409); somebody else wrote the lease
    /// between our read and write. Retry.
    Conflict,
}

/// What we last saw of the Lease, timed with the local monotonic clock.
#[derive(Debug, Clone)]
struct Observed {
    resource_version: Option<String>,
    observed_at: Instant,
    lease_duration: Duration,
}

/// Stateful elector — keeps the observation record across attempts.
pub(crate) struct LeaderElector {
    config: LeaderElectionConfig,
    api: Api<Lease>,
    observed: Option<Observed>,
}

impl LeaderElector {
    pub(crate) fn new(config: LeaderElectionConfig, client: Client) -> Self {
        let api = Api::namespaced(client, &config.namespace);
        Self {
            config,
            api,
            observed: None,
        }
    }

    /// Record the lease's current state. The observation time only moves when
    /// the `resourceVersion` changes, which is what makes expiry detection
    /// independent of the writer's clock.
    fn observe(&mut self, lease: &Lease, at: Instant) {
        let rv = lease.metadata.resource_version.clone();
        let changed = self
            .observed
            .as_ref()
            .is_none_or(|o| o.resource_version != rv);
        if changed {
            let lease_duration = lease
                .spec
                .as_ref()
                .and_then(|s| s.lease_duration_seconds)
                .and_then(|s| u64::try_from(s).ok())
                .map_or(self.config.lease_duration, Duration::from_secs);
            self.observed = Some(Observed {
                resource_version: rv,
                observed_at: at,
                lease_duration,
            });
        }
    }

    /// Test hook: pretend the current observation happened `by` earlier.
    #[cfg(test)]
    pub(crate) fn backdate_observation(&mut self, by: Duration) {
        if let Some(o) = self.observed.as_mut() {
            o.observed_at = o.observed_at.checked_sub(by).unwrap_or(o.observed_at);
        }
    }

    fn observed_expired(&self, now: Instant) -> bool {
        self.observed
            .as_ref()
            .is_none_or(|o| now.saturating_duration_since(o.observed_at) >= o.lease_duration)
    }

    /// One acquire-or-renew round trip (GET, then POST or PUT).
    pub(crate) async fn try_acquire_or_renew(&mut self) -> Result<Attempt> {
        // Captured *before* any request: for our own writes this makes the
        // recorded renewal time conservative (earlier than the real write).
        let started = Instant::now();
        let now_micro = MicroTime(k8s_openapi::jiff::Timestamp::now());
        let identity = self.config.identity.clone();

        let Some(existing) = self.api.get_opt(&self.config.name).await? else {
            let lease = build_lease(
                &self.config,
                Some(&identity),
                self.config.lease_duration_secs(),
                now_micro.clone(),
                now_micro,
                None,
                Some(0),
            );
            return match self.api.create(&PostParams::default(), &lease).await {
                Ok(created) => {
                    self.observe(&created, started);
                    Ok(Attempt::Held)
                }
                Err(kube::Error::Api(e)) if e.code == 409 => Ok(Attempt::Conflict),
                Err(e) => Err(e.into()),
            };
        };

        self.observe(&existing, started);

        let spec = existing.spec.as_ref();
        let holder = spec
            .and_then(|s| s.holder_identity.as_deref())
            .filter(|h| !h.is_empty());
        let we_hold = holder == Some(identity.as_str());

        if let Some(other) = holder
            && !we_hold
            && !self.observed_expired(started)
        {
            return Ok(Attempt::HeldByOther(other.to_string()));
        }

        let acquire_time = if we_hold {
            spec.and_then(|s| s.acquire_time.clone())
                .unwrap_or_else(|| now_micro.clone())
        } else {
            now_micro.clone()
        };
        let transitions = spec.and_then(|s| s.lease_transitions).unwrap_or(0);
        let transitions = if we_hold {
            transitions
        } else {
            transitions.saturating_add(1)
        };

        let lease = build_lease(
            &self.config,
            Some(&identity),
            self.config.lease_duration_secs(),
            acquire_time,
            now_micro,
            existing.metadata.resource_version.clone(),
            Some(transitions),
        );

        match self
            .api
            .replace(&self.config.name, &PostParams::default(), &lease)
            .await
        {
            Ok(replaced) => {
                self.observe(&replaced, started);
                Ok(Attempt::Held)
            }
            Err(kube::Error::Api(e)) if e.code == 409 => Ok(Attempt::Conflict),
            Err(e) => Err(e.into()),
        }
    }

    /// Block until this identity holds the lease.
    ///
    /// Each attempt is bounded by `renew_deadline`; errors are logged and retried.
    pub(crate) async fn acquire(&mut self) {
        info!(
            identity = %self.config.identity,
            lease = %self.config.name,
            ns = %self.config.namespace,
            "Waiting to become leader"
        );
        loop {
            match tokio::time::timeout(self.config.renew_deadline, self.try_acquire_or_renew())
                .await
            {
                Ok(Ok(Attempt::Held)) => {
                    info!(identity = %self.config.identity, "Acquired leader lease");
                    return;
                }
                Ok(Ok(Attempt::HeldByOther(holder))) => {
                    debug!(identity = %self.config.identity, %holder, "Lease held by another replica");
                }
                Ok(Ok(Attempt::Conflict)) => {
                    debug!(identity = %self.config.identity, "Lease write conflict, retrying");
                }
                Ok(Err(e)) => warn!(error = %e, "Error acquiring leader lease, retrying"),
                Err(_) => warn!(
                    timeout = ?self.config.renew_deadline,
                    "Leader lease request timed out, retrying"
                ),
            }
            tokio::time::sleep(self.config.retry_period).await;
        }
    }

    /// Keep renewing the lease. Returns only when leadership is lost, with the
    /// reason as a [`KubeGenericError::LeaderElectionLost`].
    ///
    /// Must be called right after [`acquire`](Self::acquire) succeeded.
    pub(crate) async fn renew_until_lost(&mut self) -> KubeGenericError {
        let mut last_renew = self
            .observed
            .as_ref()
            .map_or_else(Instant::now, |o| o.observed_at);
        loop {
            tokio::time::sleep(self.config.retry_period).await;

            let deadline = last_renew + self.config.renew_deadline;
            let attempt_start = Instant::now();
            if attempt_start >= deadline {
                return lost(format!(
                    "could not renew lease within renew deadline {:?}",
                    self.config.renew_deadline
                ));
            }

            let attempt = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                self.try_acquire_or_renew(),
            )
            .await;
            match attempt {
                Ok(Ok(Attempt::Held)) => {
                    last_renew = attempt_start;
                    debug!(identity = %self.config.identity, "Leader lease renewed");
                }
                Ok(Ok(Attempt::HeldByOther(holder))) => {
                    return lost(format!("lease is now held by '{holder}'"));
                }
                Ok(Ok(Attempt::Conflict)) => {
                    debug!(identity = %self.config.identity, "Lease renew conflict, retrying");
                }
                Ok(Err(e)) => warn!(error = %e, "Failed to renew leader lease, retrying"),
                Err(_) => {
                    return lost(format!(
                        "lease renewal did not complete within renew deadline {:?}",
                        self.config.renew_deadline
                    ));
                }
            }
        }
    }

    /// Give up the lease so a standby can take over immediately.
    ///
    /// No-op if another identity holds the lease. Uses `resourceVersion`
    /// optimistic locking, so a lease that changed hands in the meantime is
    /// never overwritten.
    pub(crate) async fn release(&mut self) -> Result<()> {
        let Some(existing) = self.api.get_opt(&self.config.name).await? else {
            return Ok(());
        };
        let spec = existing.spec.as_ref();
        if spec.and_then(|s| s.holder_identity.as_deref()) != Some(self.config.identity.as_str()) {
            return Ok(());
        }
        let now_micro = MicroTime(k8s_openapi::jiff::Timestamp::now());
        let lease = build_lease(
            &self.config,
            None,
            1,
            spec.and_then(|s| s.acquire_time.clone())
                .unwrap_or_else(|| now_micro.clone()),
            now_micro,
            existing.metadata.resource_version.clone(),
            spec.and_then(|s| s.lease_transitions),
        );
        self.api
            .replace(&self.config.name, &PostParams::default(), &lease)
            .await?;
        info!(identity = %self.config.identity, "Released leader lease");
        Ok(())
    }
}

fn lost(reason: String) -> KubeGenericError {
    KubeGenericError::LeaderElectionLost(reason)
}

pub(crate) fn build_lease(
    config: &LeaderElectionConfig,
    holder: Option<&str>,
    lease_duration_secs: i32,
    acquire_time: MicroTime,
    renew_time: MicroTime,
    resource_version: Option<String>,
    lease_transitions: Option<i32>,
) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(config.name.clone()),
            namespace: Some(config.namespace.clone()),
            resource_version,
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            holder_identity: holder.map(str::to_string),
            lease_duration_seconds: Some(lease_duration_secs),
            acquire_time: Some(acquire_time),
            renew_time: Some(renew_time),
            lease_transitions,
            ..Default::default()
        }),
    }
}
