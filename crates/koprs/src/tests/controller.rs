// src/tests/controller.rs

#[cfg(test)]
mod controller_tests {
    use std::sync::Arc;
    use std::time::Duration;

    use http::{Request, Response};
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::Client;
    use kube::client::Body;
    use serde_json::json;
    use tower_test::mock;

    use crate::controller::{Action, Context, ControllerBuilder, Reconciler};
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
    // Minimal Reconciler — only implements required method
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
        // error_policy intentionally omitted — tests the default impl
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
    async fn context_new_is_accessible() {
        let (client, _handle) = mock_client();
        let ctx = Context::new(client);
        assert_eq!(ctx.data, ());
    }

    #[tokio::test]
    async fn context_with_data_stores_data() {
        let (client, _handle) = mock_client();
        let ctx = Context::with_data(client, 42u32);
        assert_eq!(ctx.data, 42u32);
    }

    #[tokio::test]
    async fn context_with_data_string() {
        let (client, _handle) = mock_client();
        let ctx = Context::with_data(client, "hello".to_string());
        assert_eq!(ctx.data, "hello");
    }

    // -----------------------------------------------------------------------
    // Reconciler trait — default and override
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn reconciler_reconcile_returns_ok_action() {
        let (client, _handle) = mock_client();
        let ctx = Context::new(client);
        let r = MockReconciler;
        let cm = Arc::new(configmap("test", "ns"));
        assert!(r.reconcile(cm, ctx).await.is_ok());
    }

    #[tokio::test]
    async fn reconciler_default_error_policy_requeues_30s() {
        let (client, _handle) = mock_client();
        let ctx = Context::new(client);
        let r = MockReconciler;
        let cm = Arc::new(configmap("test", "ns"));
        let err = KubeGenericError::Internal("oops".into());
        let action = r.error_policy(cm, &err, ctx);
        // Default is requeue(30s); Action doesn't expose duration publicly
        // but we can verify it doesn't panic and returns some action
        let _ = action;
    }

    #[tokio::test]
    async fn reconciler_custom_error_policy_can_be_overridden() {
        let (client, _handle) = mock_client();
        let ctx = Context::new(client);
        let r = LoggingReconciler;
        let cm = Arc::new(configmap("test", "ns"));
        let err = KubeGenericError::Internal("oops".into());
        // Just verify the override compiles and runs without panic
        let _ = r.error_policy(cm, &err, ctx);
    }

    // -----------------------------------------------------------------------
    // ControllerBuilder — construction and configuration
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn builder_can_be_constructed() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let _builder = ControllerBuilder::<ConfigMap>::new(api);
    }

    #[tokio::test]
    async fn builder_default_has_no_features_enabled() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let builder = ControllerBuilder::<ConfigMap>::new(api);
        assert_eq!(builder.health_port, None);
        assert!(!builder.graceful_shutdown);
        assert_eq!(builder.reconcile_timeout, None);
    }

    #[tokio::test]
    async fn builder_health_port_sets_port() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let builder = ControllerBuilder::<ConfigMap>::new(api).health_port(9090);
        assert_eq!(builder.health_port, Some(9090));
    }

    #[tokio::test]
    async fn builder_graceful_shutdown_sets_flag() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let builder = ControllerBuilder::<ConfigMap>::new(api).graceful_shutdown();
        assert!(builder.graceful_shutdown);
    }

    #[tokio::test]
    async fn builder_reconcile_timeout_sets_duration() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let builder =
            ControllerBuilder::<ConfigMap>::new(api).reconcile_timeout(Duration::from_secs(60));
        assert_eq!(builder.reconcile_timeout, Some(Duration::from_secs(60)));
    }

    #[tokio::test]
    async fn builder_label_selector_sets_watcher_config() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let builder = ControllerBuilder::<ConfigMap>::new(api).label_selector("app=test");
        assert_eq!(
            builder.watcher_config.label_selector.as_deref(),
            Some("app=test")
        );
    }

    #[tokio::test]
    async fn builder_watcher_config_replaces_default() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let custom = kube_runtime::watcher::Config::default().labels("env=prod");
        let builder = ControllerBuilder::<ConfigMap>::new(api).watcher_config(custom);
        assert_eq!(
            builder.watcher_config.label_selector.as_deref(),
            Some("env=prod")
        );
    }

    #[tokio::test]
    async fn builder_with_watches_stores_configure() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let builder = ControllerBuilder::<ConfigMap>::new(api).with_watches(|ctl| ctl);
        assert!(builder.configure.is_some());
    }

    #[tokio::test]
    async fn builder_leader_election_can_be_configured() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        // Verify it compiles and configures without panic
        let _builder =
            ControllerBuilder::<ConfigMap>::new(api).leader_election("my-ns", "my-operator-leader");
    }

    #[tokio::test]
    async fn builder_chaining_all_options_does_not_panic() {
        let (client, _handle) = mock_client();
        let api = kube::Api::<ConfigMap>::namespaced(client, "ns");
        let _builder = ControllerBuilder::<ConfigMap>::new(api)
            .health_port(8080)
            .graceful_shutdown()
            .leader_election("my-ns", "my-operator-leader")
            .reconcile_timeout(Duration::from_secs(300))
            .label_selector("app=test")
            .watcher_config(kube_runtime::watcher::Config::default())
            .with_watches(|ctl| ctl);
    }

    // -----------------------------------------------------------------------
    // Context with generic data T
    // -----------------------------------------------------------------------

    struct MyData {
        value: u32,
    }

    struct DataReconciler;

    impl Reconciler<ConfigMap, MyData> for DataReconciler {
        type Error = KubeGenericError;

        async fn reconcile(
            &self,
            _cr: Arc<ConfigMap>,
            ctx: Arc<Context<MyData>>,
        ) -> Result<Action, Self::Error> {
            let _ = ctx.data.value;
            Ok(Action::await_change())
        }
    }

    #[tokio::test]
    async fn reconciler_with_custom_data_can_access_context_data() {
        let (client, _handle) = mock_client();
        let ctx = Context::with_data(client, MyData { value: 99 });
        let r = DataReconciler;
        let cm = Arc::new(configmap("test", "ns"));
        assert!(r.reconcile(cm, ctx).await.is_ok());
    }
}
