// src/tests/leader.rs

#[cfg(test)]
mod leader_tests {
    use std::time::Duration;

    use http::{Method, Request, Response, StatusCode};
    use k8s_openapi::api::coordination::v1::Lease;
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::error::KubeGenericError;
    use crate::leader::{Attempt, LeaderElectionConfig, LeaderElector};

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (Client::new(svc, "default"), handle)
    }

    fn json_response(status: StatusCode, body: serde_json::Value) -> Response<Body> {
        Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn status_response(code: u16, reason: &str) -> Response<Body> {
        json_response(
            StatusCode::from_u16(code).unwrap(),
            json!({ "apiVersion": "v1", "kind": "Status", "status": "Failure",
                    "reason": reason, "code": code }),
        )
    }

    async fn read_body_json(req: Request<Body>) -> serde_json::Value {
        use http_body_util::BodyExt as _;
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// A Lease body. `renew_offset_secs` is relative to now (negative = past).
    fn lease_json(
        holder: Option<&str>,
        lease_duration_secs: i32,
        renew_offset_secs: i64,
        resource_version: &str,
        transitions: i32,
    ) -> serde_json::Value {
        use chrono::SecondsFormat;
        let t = (chrono::Utc::now() + chrono::Duration::seconds(renew_offset_secs))
            .to_rfc3339_opts(SecondsFormat::Micros, true);
        let mut spec = json!({
            "leaseDurationSeconds": lease_duration_secs,
            "acquireTime": "2020-01-01T00:00:00.000000Z",
            "renewTime": t,
            "leaseTransitions": transitions
        });
        if let Some(h) = holder {
            spec["holderIdentity"] = json!(h);
        }
        json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": "test-lease", "namespace": "test-ns",
                          "resourceVersion": resource_version },
            "spec": spec
        })
    }

    fn config(identity: &str) -> LeaderElectionConfig {
        let mut c =
            LeaderElectionConfig::new("test-ns".into(), "test-lease".into(), identity.into());
        c.lease_duration = Duration::from_secs(15);
        c.renew_deadline = Duration::from_millis(300);
        c.retry_period = Duration::from_millis(20);
        c
    }

    fn elector(identity: &str) -> (LeaderElector, MockHandle) {
        let (client, handle) = mock_client();
        (LeaderElector::new(config(identity), client), handle)
    }

    // -----------------------------------------------------------------------
    // Config validation
    // -----------------------------------------------------------------------

    #[test]
    fn default_config_is_valid() {
        let c = LeaderElectionConfig::new("ns".into(), "l".into(), "pod".into());
        c.validate().unwrap();
        assert_eq!(c.lease_duration_secs(), 15);
    }

    #[test]
    fn validate_rejects_unsafe_timings() {
        let mut c = config("pod");
        c.renew_deadline = c.lease_duration;
        assert!(matches!(c.validate(), Err(KubeGenericError::Config(_))));

        let mut c = config("pod");
        c.retry_period = c.renew_deadline;
        assert!(matches!(c.validate(), Err(KubeGenericError::Config(_))));

        let mut c = config("pod");
        c.retry_period = Duration::ZERO;
        assert!(matches!(c.validate(), Err(KubeGenericError::Config(_))));

        let mut c = config("");
        c.identity.clear();
        assert!(matches!(c.validate(), Err(KubeGenericError::Config(_))));
    }

    #[test]
    fn lease_duration_secs_rounds_up_and_never_truncates() {
        let mut c = config("pod");
        c.lease_duration = Duration::from_millis(15_500);
        assert_eq!(c.lease_duration_secs(), 16);
        c.lease_duration = Duration::from_secs(u64::MAX / 2);
        assert_eq!(c.lease_duration_secs(), i32::MAX);
    }

    // -----------------------------------------------------------------------
    // try_acquire_or_renew
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn creates_lease_when_none_exists() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::GET);
            send.send_response(status_response(404, "NotFound"));

            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::POST);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            assert_eq!(body["spec"]["leaseDurationSeconds"], 15);
            assert_eq!(body["spec"]["leaseTransitions"], 0);
            send.send_response(json_response(
                StatusCode::CREATED,
                lease_json(Some("pod-1"), 15, 0, "1", 0),
            ));
        });
        assert_eq!(el.try_acquire_or_renew().await.unwrap(), Attempt::Held);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn create_conflict_is_reported_as_conflict() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(404, "NotFound"));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(409, "AlreadyExists"));
        });
        assert_eq!(el.try_acquire_or_renew().await.unwrap(), Attempt::Conflict);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn stale_wall_clock_renew_time_does_not_make_a_fresh_lease_look_expired() {
        // The holder's renewTime is an hour in the past (e.g. its clock is
        // skewed). A candidate that has just seen this resourceVersion must
        // still treat the lease as held — expiry is local-observation based.
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("other-pod"), 15, -3600, "5", 0),
            ));
        });
        assert_eq!(
            el.try_acquire_or_renew().await.unwrap(),
            Attempt::HeldByOther("other-pod".into())
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn takes_over_once_lease_unchanged_for_lease_duration() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            // First observation of rv 5.
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("other-pod"), 15, 0, "5", 2),
            ));
            // Same rv 5 again, after lease_duration has passed locally.
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("other-pod"), 15, 0, "5", 2),
            ));
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::PUT);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            assert_eq!(body["metadata"]["resourceVersion"], "5");
            assert_eq!(
                body["spec"]["leaseTransitions"], 3,
                "takeover increments transitions"
            );
            assert_ne!(body["spec"]["acquireTime"], "2020-01-01T00:00:00.000000Z");
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "6", 3),
            ));
        });
        assert!(matches!(
            el.try_acquire_or_renew().await.unwrap(),
            Attempt::HeldByOther(_)
        ));
        el.backdate_observation(Duration::from_secs(16));
        assert_eq!(el.try_acquire_or_renew().await.unwrap(), Attempt::Held);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn resource_version_change_restarts_the_expiry_clock() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("other-pod"), 15, 0, "5", 0),
            ));
            // The holder renewed (rv 6): the lease is alive again.
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("other-pod"), 15, 0, "6", 0),
            ));
        });
        el.try_acquire_or_renew().await.unwrap();
        el.backdate_observation(Duration::from_secs(16));
        assert_eq!(
            el.try_acquire_or_renew().await.unwrap(),
            Attempt::HeldByOther("other-pod".into())
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn released_lease_is_acquired_immediately() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(None, 1, 0, "7", 4),
            ));
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::PUT);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            assert_eq!(body["spec"]["leaseTransitions"], 5);
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "8", 5),
            ));
        });
        assert_eq!(el.try_acquire_or_renew().await.unwrap(), Attempt::Held);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn renew_preserves_acquire_time_and_transitions() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, -3, "9", 2),
            ));
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::PUT);
            let body = read_body_json(req).await;
            assert_eq!(body["spec"]["holderIdentity"], "pod-1");
            assert_eq!(body["spec"]["acquireTime"], "2020-01-01T00:00:00.000000Z");
            assert_eq!(body["spec"]["leaseTransitions"], 2);
            assert_eq!(body["metadata"]["resourceVersion"], "9");
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "10", 2),
            ));
        });
        assert_eq!(el.try_acquire_or_renew().await.unwrap(), Attempt::Held);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn replace_conflict_is_reported_as_conflict() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, -3, "9", 0),
            ));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(409, "Conflict"));
        });
        assert_eq!(el.try_acquire_or_renew().await.unwrap(), Attempt::Conflict);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn api_errors_propagate() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
        });
        assert!(el.try_acquire_or_renew().await.is_err());
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // renew_until_lost
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn renew_until_lost_returns_when_another_replica_holds_the_lease() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            // Renewal #1 succeeds.
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "1", 0),
            ));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "2", 0),
            ));
            // Renewal #2 finds a different holder.
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-2"), 15, 0, "3", 1),
            ));
        });
        let err = el.renew_until_lost().await;
        match err {
            KubeGenericError::LeaderElectionLost(msg) => assert!(msg.contains("pod-2"), "{msg}"),
            other => panic!("unexpected {other:?}"),
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn renew_until_lost_steps_down_when_the_api_hangs() {
        // The API server accepts the request but never answers. The leader
        // must give up within the renew deadline instead of hanging forever.
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_req, _send) = handle.next_request().await.unwrap();
            // Hold the responder until the elector gives up.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let started = std::time::Instant::now();
        let err = el.renew_until_lost().await;
        assert!(
            matches!(err, KubeGenericError::LeaderElectionLost(_)),
            "got {err:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "took {:?}, expected roughly the 300ms renew deadline",
            started.elapsed()
        );
        server.abort();
    }

    #[tokio::test]
    async fn renew_until_lost_retries_transient_errors_within_the_deadline() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            // One failure, then a successful renewal, then the lease is taken.
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(status_response(500, "InternalError"));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "1", 0),
            ));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "2", 0),
            ));
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-2"), 15, 0, "3", 1),
            ));
        });
        let err = el.renew_until_lost().await;
        match err {
            KubeGenericError::LeaderElectionLost(msg) => assert!(msg.contains("pod-2"), "{msg}"),
            other => panic!("unexpected {other:?}"),
        }
        server.await.unwrap();
    }

    // -----------------------------------------------------------------------
    // release
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn release_clears_holder_and_shortens_duration() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-1"), 15, 0, "4", 1),
            ));
            let (req, send) = handle.next_request().await.unwrap();
            assert_eq!(req.method(), Method::PUT);
            let body = read_body_json(req).await;
            assert!(body["spec"].get("holderIdentity").is_none(), "body={body}");
            assert_eq!(body["spec"]["leaseDurationSeconds"], 1);
            assert_eq!(body["metadata"]["resourceVersion"], "4");
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(None, 1, 0, "5", 1),
            ));
        });
        el.release().await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn release_does_not_touch_a_lease_held_by_someone_else() {
        let (mut el, mut handle) = elector("pod-1");
        let server = tokio::spawn(async move {
            let (_, send) = handle.next_request().await.unwrap();
            send.send_response(json_response(
                StatusCode::OK,
                lease_json(Some("pod-2"), 15, 0, "4", 1),
            ));
            // No PUT may follow.
            let next =
                tokio::time::timeout(Duration::from_millis(100), handle.next_request()).await;
            assert!(
                matches!(next, Err(_) | Ok(None)),
                "release must not write a lease it does not hold"
            );
        });
        el.release().await.unwrap();
        server.await.unwrap();
    }

    #[test]
    fn lease_json_round_trips_through_serde() {
        let lease: Lease =
            serde_json::from_value(lease_json(Some("pod-1"), 15, 0, "42", 0)).unwrap();
        assert_eq!(
            lease.spec.unwrap().holder_identity.as_deref(),
            Some("pod-1")
        );
    }
}
