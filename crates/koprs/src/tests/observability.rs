// src/tests/observability.rs

#[cfg(test)]
mod observability_tests {
    use std::time::Duration;

    use prometheus::Registry;

    use crate::observability::{Metrics, render, serve_metrics};

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    #[test]
    fn new_registered_registers_all_collectors() {
        let registry = Registry::new();
        let metrics = Metrics::new_registered(&registry).expect("registration succeeds");

        // Vec-based collectors only appear in `gather()` once a label
        // combination has been observed at least once.
        metrics.record_success("ConfigMap", Duration::from_millis(10));
        metrics.record_failure("ConfigMap", "boom", Duration::from_millis(10));

        let names: Vec<String> = registry
            .gather()
            .into_iter()
            .map(|mf| mf.name().to_string())
            .collect();

        assert!(names.contains(&"koprs_reconciliations_total".to_string()));
        assert!(names.contains(&"koprs_reconcile_errors_total".to_string()));
        assert!(names.contains(&"koprs_reconcile_duration_seconds".to_string()));
    }

    #[test]
    fn registering_twice_fails() {
        let registry = Registry::new();
        Metrics::new_registered(&registry).expect("first registration succeeds");
        let err = Metrics::new_registered(&registry).expect_err("duplicate registration fails");
        assert!(matches!(err, crate::error::KubeGenericError::Internal(_)));
    }

    #[test]
    fn record_success_increments_total_but_not_errors() {
        let registry = Registry::new();
        let metrics = Metrics::new_registered(&registry).unwrap();

        metrics.record_success("ConfigMap", Duration::from_millis(5));
        metrics.record_success("ConfigMap", Duration::from_millis(5));

        let output = render(&registry).unwrap();
        assert!(output.contains("koprs_reconciliations_total 2"));
        assert!(!output.contains("koprs_reconcile_errors_total"));
    }

    #[test]
    fn record_failure_increments_total_and_labelled_error_counter() {
        let registry = Registry::new();
        let metrics = Metrics::new_registered(&registry).unwrap();

        metrics.record_failure("ConfigMap", "not found", Duration::from_millis(1));

        let output = render(&registry).unwrap();
        assert!(output.contains("koprs_reconciliations_total 1"));
        assert!(
            output
                .contains(r#"koprs_reconcile_errors_total{error="not found",kind="ConfigMap"} 1"#)
        );
    }

    #[test]
    fn render_includes_duration_histogram_for_recorded_kind() {
        let registry = Registry::new();
        let metrics = Metrics::new_registered(&registry).unwrap();

        metrics.record_success("ConfigMap", Duration::from_millis(100));

        let output = render(&registry).unwrap();
        assert!(output.contains("koprs_reconcile_duration_seconds_count{kind=\"ConfigMap\"} 1"));
    }

    #[test]
    fn render_on_empty_registry_is_empty() {
        let registry = Registry::new();
        assert_eq!(render(&registry).unwrap(), "");
    }

    // -----------------------------------------------------------------------
    // serve_metrics — exercised via real TCP
    // -----------------------------------------------------------------------

    /// Bind a random port, start serve_metrics, return the port.
    async fn start_metrics_server(registry: Registry) -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(serve_metrics(listener, registry));
        port
    }

    /// Send a minimal HTTP/1.1 GET and return the full raw response.
    async fn http_get(port: u16, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn metrics_endpoint_returns_200_and_renders_registry() {
        let registry = Registry::new();
        let metrics = Metrics::new_registered(&registry).unwrap();
        metrics.record_success("ConfigMap", Duration::from_millis(20));

        let port = start_metrics_server(registry).await;
        let resp = http_get(port, "/metrics").await;

        assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
        assert!(
            resp.contains("koprs_reconciliations_total 1"),
            "expected metric in body, got: {resp}"
        );
    }

    #[tokio::test]
    async fn unknown_path_returns_404() {
        let registry = Registry::new();
        let port = start_metrics_server(registry).await;
        let resp = http_get(port, "/nope").await;
        assert!(resp.starts_with("HTTP/1.1 404"), "got: {resp}");
    }
}
