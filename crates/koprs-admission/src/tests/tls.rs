// src/tests/tls.rs

#[cfg(test)]
mod tls_tests {
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::Duration;

    use rustls::pki_types::{CertificateDer, ServerName};
    use tokio::net::TcpListener;
    use tokio_rustls::{TlsAcceptor, TlsConnector};

    use crate::tls::ReloadingCertResolver;
    use crate::webhook::WebhookBuilder;

    // -----------------------------------------------------------------------
    // Fixtures
    // -----------------------------------------------------------------------

    struct Cert {
        cert_pem: String,
        key_pem: String,
        der: CertificateDer<'static>,
    }

    fn self_signed() -> Cert {
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        Cert {
            cert_pem: ck.cert.pem(),
            key_pem: ck.key_pair.serialize_pem(),
            der: ck.cert.der().clone(),
        }
    }

    /// A unique, empty temp directory (removed on drop).
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let dir = std::env::temp_dir().join(format!(
                "koprs-admission-tls-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn write_cert(dir: &TempDir, cert: &Cert) -> (PathBuf, PathBuf) {
        let (c, k) = (dir.path("tls.crt"), dir.path("tls.key"));
        std::fs::write(&c, &cert.cert_pem).unwrap();
        std::fs::write(&k, &cert.key_pem).unwrap();
        // Make sure the modification time visibly changes between writes even
        // on filesystems with coarse timestamps.
        bump_mtime(&c);
        bump_mtime(&k);
        (c, k)
    }

    fn bump_mtime(path: &Path) {
        use std::sync::atomic::{AtomicU64, Ordering};
        static T: AtomicU64 = AtomicU64::new(1_000_000);
        let secs = T.fetch_add(10, Ordering::Relaxed);
        let file = std::fs::File::options().write(true).open(path).unwrap();
        file.set_modified(std::time::UNIX_EPOCH + Duration::from_secs(secs))
            .unwrap();
    }

    fn leaf(resolver: &ReloadingCertResolver) -> CertificateDer<'static> {
        resolver.current().cert[0].clone()
    }

    // -----------------------------------------------------------------------
    // Static PEM
    // -----------------------------------------------------------------------

    #[test]
    fn tls_from_pem_accepts_a_matching_pair() {
        let c = self_signed();
        WebhookBuilder::new()
            .tls_from_pem(c.cert_pem.as_bytes(), c.key_pem.as_bytes())
            .unwrap();
    }

    #[test]
    fn tls_from_pem_rejects_a_key_that_does_not_match_the_certificate() {
        let a = self_signed();
        let b = self_signed();
        let result =
            WebhookBuilder::new().tls_from_pem(a.cert_pem.as_bytes(), b.key_pem.as_bytes());
        assert!(result.is_err(), "mismatched key must be rejected");
    }

    #[test]
    fn tls_from_pem_rejects_empty_input() {
        assert!(WebhookBuilder::new().tls_from_pem(b"", b"").is_err());
    }

    // -----------------------------------------------------------------------
    // ReloadingCertResolver
    // -----------------------------------------------------------------------

    #[test]
    fn reloading_resolver_fails_fast_when_files_are_missing() {
        let dir = TempDir::new();
        let result = ReloadingCertResolver::new(
            dir.path("missing.crt"),
            dir.path("missing.key"),
            Duration::from_secs(10),
        );
        assert!(result.is_err());
    }

    #[test]
    fn reloading_resolver_picks_up_a_rotated_certificate() {
        let dir = TempDir::new();
        let first = self_signed();
        let (c, k) = write_cert(&dir, &first);
        let resolver = ReloadingCertResolver::new(&c, &k, Duration::from_secs(3600)).unwrap();
        assert_eq!(leaf(&resolver), first.der);

        let second = self_signed();
        write_cert(&dir, &second);
        resolver.force_check();
        assert_eq!(
            leaf(&resolver),
            second.der,
            "rotated certificate not picked up"
        );
    }

    #[test]
    fn reloading_resolver_keeps_serving_the_old_certificate_when_reload_fails() {
        let dir = TempDir::new();
        let first = self_signed();
        let (c, k) = write_cert(&dir, &first);
        let resolver = ReloadingCertResolver::new(&c, &k, Duration::from_secs(3600)).unwrap();

        // Half-written rotation: new certificate, old key.
        let second = self_signed();
        std::fs::write(&c, &second.cert_pem).unwrap();
        bump_mtime(&c);
        resolver.force_check();
        assert_eq!(
            leaf(&resolver),
            first.der,
            "must not switch to a mismatched pair"
        );

        // Rotation completes: the next check succeeds.
        std::fs::write(&k, &second.key_pem).unwrap();
        bump_mtime(&k);
        resolver.force_check();
        assert_eq!(leaf(&resolver), second.der);
    }

    // -----------------------------------------------------------------------
    // End-to-end: a running TLS listener serves the rotated certificate
    // -----------------------------------------------------------------------

    async fn handshake(port: u16, trusted: &CertificateDer<'static>) -> bool {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(trusted.clone()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        connector
            .connect(ServerName::try_from("localhost").unwrap(), stream)
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn running_server_presents_the_rotated_certificate_without_restart() {
        let dir = TempDir::new();
        let first = self_signed();
        let (c, k) = write_cert(&dir, &first);

        let builder = WebhookBuilder::new()
            .tls_from_files_with_interval(&c, &k, Duration::from_millis(50))
            .unwrap();
        let acceptor = TlsAcceptor::from(builder.tls.clone().unwrap());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (_tx, rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(crate::webhook::serve(
            listener,
            Some(acceptor),
            axum::Router::new(),
            rx,
        ));

        assert!(
            handshake(port, &first.der).await,
            "initial certificate not served"
        );

        let second = self_signed();
        write_cert(&dir, &second);
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(
            handshake(port, &second.der).await,
            "rotated certificate not served"
        );
        assert!(
            !handshake(port, &first.der).await,
            "old certificate still served"
        );

        server.abort();
    }
}
