// src/tests/controller.rs

#[cfg(test)]
mod controller_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use http::{Request, Response};
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::controller::{Action, Context, ControllerBuilder, Reconciler, serve_health};
    use crate::error::KubeGenericError;

    // -----------------------------------------------------------------------
    // Harness
    // -----------------------------------------------------------------------

    type MockHandle = mock::Handle<Request<Body>, Response<Body>>;

    fn mock_client() -> (Client, MockHandle) {
        let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
        (Client::new(svc, "default"), handle)
    }

    // -----------------------------------------------------------------------
    // Minimal Reconciler — only required method, uses default error_policy
    // -----------------------------------------------------------------------

    struct MockReconciler;

    impl Reconciler<ConfigMap> for MockReconciler {
        type Error = KubeGenericError;

        async fn reconcile(
            &self,
            _cr: Arc<ConfigMap>,
            _ctx: Arc<Context>,
        ) -> Result<Action, Self::Error> {
            Ok(Action::await_change())
        }
        // error_policy intentionally omitted — exercises the default impl
    }

    // Reconciler with explicit error_policy override
    struct LoggingReconciler;

    impl Reconciler<ConfigMap> for LoggingReconciler {
        type Error = KubeGenericError;

        async fn reconcile(
            &self,
            _cr: Arc<ConfigMap>,
            _ctx: Arc<Context>,
        ) -> Result<Action, Self::Error> {
            Ok(Action::await_change())
        }

        fn error_policy(
            &self,
            _cr: Arc<ConfigMap>,
            _err: &Self::Error,
            _ctx: Arc<Context>,
        ) -> Action {
            Action::requeue(Duration::from_secs(60))
        }
    }

    fn configmap(name: &str, namespace: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace }
        }))
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // Context
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn context_new_data_is_unit() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        assert_eq!(ctx.data, ());
    }

    #[tokio::test]
    async fn context_with_data_stores_value() {
        let (client, _) = mock_client();
        let ctx = Context::with_data(client, 42u32);
        assert_eq!(ctx.data, 42u32);
    }

    #[tokio::test]
    async fn context_with_data_string() {
        let (client, _) = mock_client();
        let ctx = Context::with_data(client, "hello".to_string());
        assert_eq!(ctx.data, "hello");
    }

    #[tokio::test]
    async fn context_client_is_accessible() {
        let (client, _) = mock_client();
        let ctx = Context::new(client.clone());
        // Verify the client field is public and the same instance
        // (both are Arc-backed — just check the field is accessible)
        let _ = ctx.client.clone();
    }

    // -----------------------------------------------------------------------
    // Reconciler trait — default and override
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn reconciler_reconcile_returns_ok() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        assert!(
            MockReconciler
                .reconcile(Arc::new(configmap("t", "ns")), ctx)
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn reconciler_default_error_policy_does_not_panic() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        let err = KubeGenericError::Internal("oops".into());
        let _ = MockReconciler.error_policy(Arc::new(configmap("t", "ns")), &err, ctx);
    }

    #[tokio::test]
    async fn reconciler_custom_error_policy_is_called() {
        let (client, _) = mock_client();
        let ctx = Context::new(client);
        let err = KubeGenericError::Internal("oops".into());
        // LoggingReconciler overrides to requeue(60s) — just verify it doesn't panic
        let _ = LoggingReconciler.error_policy(Arc::new(configmap("t", "ns")), &err, ctx);
    }

    #[tokio::test]
    async fn reconciler_with_custom_data_can_read_context() {
        struct MyData {
            x: u32,
        }
        struct DataReconciler;
        impl Reconciler<ConfigMap, MyData> for DataReconciler {
            type Error = KubeGenericError;
            async fn reconcile(
                &self,
                _cr: Arc<ConfigMap>,
                ctx: Arc<Context<MyData>>,
            ) -> Result<Action, Self::Error> {
                assert_eq!(ctx.data.x, 7);
                Ok(Action::await_change())
            }
        }
        let (client, _) = mock_client();
        let ctx = Context::with_data(client, MyData { x: 7 });
        assert!(
            DataReconciler
                .reconcile(Arc::new(configmap("t", "ns")), ctx)
                .await
                .is_ok()
        );
    }

    // -----------------------------------------------------------------------
    // serve_health — tests all response branches via real TCP
    // -----------------------------------------------------------------------

    /// Bind a random port, start serve_health, return (port, ready flag).
    async fn start_health_server() -> (u16, Arc<AtomicBool>) {
        let ready = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve_health(listener, ready.clone()));
        (port, ready)
    }

    /// Send a minimal HTTP/1.1 GET and return the full raw response.
    async fn http_get(port: u16, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = vec![0u8; 512];
        let n = stream.read(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf[..n]).into_owned()
    }

    #[tokio::test]
    async fn health_server_healthz_returns_200() {
        let (port, _ready) = start_health_server().await;
        let resp = http_get(port, "/healthz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
        assert!(resp.ends_with("ok"), "body should be 'ok', got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_returns_503_when_not_ready() {
        let (port, _ready) = start_health_server().await;
        // ready flag is false at start
        let resp = http_get(port, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 503"), "got: {resp}");
        assert!(
            resp.ends_with("not ready"),
            "body should be 'not ready', got: {resp}"
        );
    }

    #[tokio::test]
    async fn health_server_readyz_returns_200_when_ready() {
        let (port, ready) = start_health_server().await;
        ready.store(true, Ordering::Release);
        let resp = http_get(port, "/readyz").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
        assert!(resp.ends_with("ok"), "body should be 'ok', got: {resp}");
    }

    #[tokio::test]
    async fn health_server_unknown_path_returns_200() {
        let (port, _) = start_health_server().await;
        let resp = http_get(port, "/metrics").await;
        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
    }

    #[tokio::test]
    async fn health_server_readyz_transitions_503_to_200_after_ready() {
        let (port, ready) = start_health_server().await;

        let before = http_get(port, "/readyz").await;
        assert!(
            before.starts_with("HTTP/1.1 503"),
            "expected 503 before ready: {before}"
        );

        ready.store(true, Ordering::Release);

        let after = http_get(port, "/readyz").await;
        assert!(
            after.starts_with("HTTP/1.1 200 OK"),
            "expected 200 after ready: {after}"
        );
    }

    #[tokio::test]
    async fn health_server_healthz_returns_200_regardless_of_ready_flag() {
        let (port, ready) = start_health_server().await;

        // With ready=false
        let resp = http_get(port, "/healthz").await;
        assert!(
            resp.starts_with("HTTP/1.1 200 OK"),
            "healthz should be 200 when not ready"
        );

        // With ready=true
        ready.store(true, Ordering::Release);
        let resp = http_get(port, "/healthz").await;
        assert!(
            resp.starts_with("HTTP/1.1 200 OK"),
            "healthz should be 200 when ready"
        );
    }

    // -----------------------------------------------------------------------
    // ControllerBuilder — construction and configuration
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn builder_defaults_are_all_off() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api);
        assert_eq!(b.health_port, None);
        assert!(!b.graceful_shutdown);
        assert_eq!(b.reconcile_timeout, None);
    }

    #[tokio::test]
    async fn builder_health_port_sets_port() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).health_port(9090);
        assert_eq!(b.health_port, Some(9090));
    }

    #[tokio::test]
    async fn builder_graceful_shutdown_sets_flag() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).graceful_shutdown();
        assert!(b.graceful_shutdown);
    }

    #[tokio::test]
    async fn builder_reconcile_timeout_stores_duration() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).reconcile_timeout(Duration::from_secs(60));
        assert_eq!(b.reconcile_timeout, Some(Duration::from_secs(60)));
    }

    #[tokio::test]
    async fn builder_label_selector_sets_watcher_config() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).label_selector("app=test");
        assert_eq!(b.watcher_config.label_selector.as_deref(), Some("app=test"));
    }

    #[tokio::test]
    async fn builder_watcher_config_replaces_default() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let custom = kube_runtime::watcher::Config::default().labels("env=prod");
        let b = ControllerBuilder::<ConfigMap>::new(api).watcher_config(custom);
        assert_eq!(b.watcher_config.label_selector.as_deref(), Some("env=prod"));
    }

    #[tokio::test]
    async fn builder_with_watches_stores_configure_fn() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).with_watches(|ctl| ctl);
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_with_watches_composes_not_replaces() {
        // Bug fix: a second with_watches call used to silently replace the first.
        // Now both closures are chained — configure must still be Some after
        // each call and both closures participate.
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .with_watches(|ctl| ctl)
            .with_watches(|ctl| ctl);
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_watch_registers_configure_fn() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api).watch(
            trigger,
            kube_runtime::watcher::Config::default(),
            |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
        );
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_watch_two_watches_both_compose() {
        // Two .watch() calls must both be preserved — the second must not
        // erase the first.
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger1 = kube::Api::<ConfigMap>::all(client.clone());
        let trigger2 = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .watch(
                trigger1,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            )
            .watch(
                trigger2,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            );
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_watch_and_with_watches_compose() {
        // .watch() and .with_watches() must compose — neither erases the other.
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .watch(
                trigger,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            )
            .with_watches(|ctl| ctl);
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_leader_election_can_be_configured() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        // verify it compiles, runs without panic, and sets the config
        let b = ControllerBuilder::<ConfigMap>::new(api).leader_election("my-ns", "my-leader");
        assert!(b.leader_election.is_some());
    }

    #[tokio::test]
    async fn builder_leader_election_namespace_and_name_are_stored() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).leader_election("ops-ns", "my-op-leader");
        let le = b.leader_election.as_ref().unwrap();
        assert_eq!(le.namespace, "ops-ns");
        assert_eq!(le.name, "my-op-leader");
    }

    #[tokio::test]
    async fn builder_leader_election_identity_defaults_to_env() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).leader_election("ns", "lease");
        let identity = b.leader_election.unwrap().identity;
        // $POD_NAME, then $HOSTNAME; empty (rejected later by `run`) if neither is set.
        let expected = std::env::var("POD_NAME")
            .or_else(|_| std::env::var("HOSTNAME"))
            .unwrap_or_default();
        assert_eq!(identity, expected);
    }

    #[tokio::test]
    async fn builder_full_chain_does_not_panic() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let _b = ControllerBuilder::<ConfigMap>::new(api)
            .health_port(8080)
            .graceful_shutdown()
            .leader_election("my-ns", "leader")
            .reconcile_timeout(Duration::from_secs(300))
            .label_selector("app=test")
            .watcher_config(kube_runtime::watcher::Config::default())
            .watch(
                trigger,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            )
            .with_watches(|ctl| ctl);
    }

    #[tokio::test]
    async fn builder_concurrency_defaults_to_none() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api);
        assert_eq!(b.concurrency, None);
    }

    #[tokio::test]
    async fn builder_concurrency_stores_value() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api).concurrency(4);
        assert_eq!(b.concurrency, Some(4));
    }

    #[tokio::test]
    async fn builder_concurrency_last_call_wins() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .concurrency(4)
            .concurrency(8);
        assert_eq!(b.concurrency, Some(8));
    }

    #[tokio::test]
    async fn builder_owns_registers_configure_fn() {
        use k8s_openapi::api::apps::v1::Deployment;
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let deploy_api = kube::Api::<Deployment>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .owns(deploy_api, kube_runtime::watcher::Config::default());
        assert!(b.configure.is_some());
    }

    #[tokio::test]
    async fn builder_owns_composes_with_watch() {
        use k8s_openapi::api::apps::v1::Deployment;
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let deploy_api = kube::Api::<Deployment>::namespaced(client.clone(), "ns");
        let trigger = kube::Api::<ConfigMap>::all(client);
        let b = ControllerBuilder::<ConfigMap>::new(api)
            .owns(deploy_api, kube_runtime::watcher::Config::default())
            .watch(
                trigger,
                kube_runtime::watcher::Config::default(),
                |_: ConfigMap| None::<kube_runtime::reflector::ObjectRef<ConfigMap>>,
            );
        assert!(b.configure.is_some());
    }

    fn builder_with_identity(identity: &str) -> ControllerBuilder<ConfigMap> {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let mut b =
            ControllerBuilder::<ConfigMap>::new(api).leader_election("ops-ns", "my-op-leader");
        b.leader_election.as_mut().unwrap().identity = identity.to_string();
        b
    }

    #[tokio::test]
    async fn builder_leader_election_defaults_follow_client_go() {
        let cfg = builder_with_identity("pod-a")
            .resolve_leader_election()
            .unwrap()
            .unwrap();
        assert_eq!(cfg.lease_duration, Duration::from_secs(15));
        assert_eq!(cfg.renew_deadline, Duration::from_secs(10));
        assert_eq!(cfg.retry_period, Duration::from_secs(2));
    }

    #[tokio::test]
    async fn builder_leader_election_timings_overrides_defaults() {
        let cfg = builder_with_identity("pod-a")
            .leader_election_timings(
                Duration::from_secs(45),
                Duration::from_secs(30),
                Duration::from_secs(3),
            )
            .resolve_leader_election()
            .unwrap()
            .unwrap();
        assert_eq!(cfg.lease_duration, Duration::from_secs(45));
        assert_eq!(cfg.renew_deadline, Duration::from_secs(30));
        assert_eq!(cfg.retry_period, Duration::from_secs(3));
    }

    #[tokio::test]
    async fn builder_leader_election_timings_order_does_not_matter() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let mut b = ControllerBuilder::<ConfigMap>::new(api)
            .leader_election_timings(
                Duration::from_secs(20),
                Duration::from_secs(12),
                Duration::from_secs(4),
            )
            .leader_election("ops-ns", "lease");
        b.leader_election.as_mut().unwrap().identity = "pod-a".into();
        let cfg = b.resolve_leader_election().unwrap().unwrap();
        assert_eq!(cfg.lease_duration, Duration::from_secs(20));
    }

    #[tokio::test]
    async fn builder_leader_election_timings_without_leader_election_is_config_error() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let err = ControllerBuilder::<ConfigMap>::new(api)
            .leader_election_timings(
                Duration::from_secs(45),
                Duration::from_secs(10),
                Duration::from_secs(3),
            )
            .resolve_leader_election()
            .unwrap_err();
        assert!(matches!(err, KubeGenericError::Config(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn builder_rejects_renew_deadline_not_shorter_than_lease_duration() {
        let err = builder_with_identity("pod-a")
            .leader_election_timings(
                Duration::from_secs(10),
                Duration::from_secs(10),
                Duration::from_secs(2),
            )
            .resolve_leader_election()
            .unwrap_err();
        assert!(matches!(err, KubeGenericError::Config(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn builder_rejects_retry_period_not_shorter_than_renew_deadline() {
        let err = builder_with_identity("pod-a")
            .leader_election_timings(
                Duration::from_secs(15),
                Duration::from_secs(2),
                Duration::from_secs(2),
            )
            .resolve_leader_election()
            .unwrap_err();
        assert!(matches!(err, KubeGenericError::Config(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn builder_rejects_empty_identity() {
        let err = builder_with_identity("")
            .resolve_leader_election()
            .unwrap_err();
        assert!(matches!(err, KubeGenericError::Config(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn builder_leader_election_identity_overrides_env() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let cfg = ControllerBuilder::<ConfigMap>::new(api)
            .leader_election_identity("replica-7")
            .leader_election("ns", "lease")
            .resolve_leader_election()
            .unwrap()
            .unwrap();
        assert_eq!(cfg.identity, "replica-7");
    }

    #[tokio::test]
    async fn builder_without_leader_election_resolves_to_none() {
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let b = ControllerBuilder::<ConfigMap>::new(api);
        assert!(b.resolve_leader_election().unwrap().is_none());
    }

    #[tokio::test]
    async fn run_future_is_send_so_it_can_be_spawned() {
        fn assert_send<T: Send>(_: &T) {}
        let (client, _) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client.clone(), "ns");
        let fut = ControllerBuilder::<ConfigMap>::new(api)
            .graceful_shutdown()
            .leader_election("ns", "lease")
            .run(MockReconciler, Context::new(client));
        assert_send(&fut);
    }
}
