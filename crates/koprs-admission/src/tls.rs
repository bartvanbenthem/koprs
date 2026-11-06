//! TLS configuration helpers, including certificate hot-reload.
//!
//! [`ReloadingCertResolver`] serves a certificate read from disk and re-reads
//! it when the files change, so certificates rotated by cert-manager (or any
//! process that rewrites the mounted Secret) are picked up without restarting
//! the webhook pod. Kubernetes updates Secret volumes by atomically swapping a
//! symlink, which changes the modification time seen through the symlink.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime};

use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use tracing::{info, warn};

use crate::error::{AdmissionError, Result};

/// How often [`ReloadingCertResolver`] checks the files for changes.
pub const DEFAULT_RELOAD_INTERVAL: Duration = Duration::from_secs(10);

/// The crypto provider used for all server configs built by this crate.
///
/// Explicit rather than process-default so building a config never panics
/// when several rustls providers are compiled into the binary.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Parse a PEM certificate chain and private key into a [`CertifiedKey`],
/// verifying that the key matches the leaf certificate.
pub(crate) fn certified_key_from_pem(
    cert_pem: &[u8],
    key_pem: &[u8],
    provider: &CryptoProvider,
) -> Result<CertifiedKey> {
    use rustls::pki_types::pem::{self, PemObject};

    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(cert_pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| AdmissionError::Tls(e.to_string()))?;
    if certs.is_empty() {
        return Err(AdmissionError::Tls(
            "no certificates found in PEM data".into(),
        ));
    }
    let key: PrivateKeyDer<'static> =
        PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| match e {
            pem::Error::NoItemsFound => {
                AdmissionError::Tls("no private key found in PEM data".into())
            }
            e => AdmissionError::Tls(e.to_string()),
        })?;

    CertifiedKey::from_der(certs, key, provider).map_err(|e| AdmissionError::Tls(e.to_string()))
}

/// Build a [`ServerConfig`] around any certificate resolver.
pub(crate) fn server_config(resolver: Arc<dyn ResolvesServerCert>) -> Result<ServerConfig> {
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| AdmissionError::Tls(e.to_string()))?
        .with_no_client_auth()
        .with_cert_resolver(resolver);
    // The API server talks HTTP/1.1 to webhooks.
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(config)
}

/// A resolver that always returns the same certificate.
#[derive(Debug)]
pub(crate) struct StaticCertResolver(pub(crate) Arc<CertifiedKey>);

impl ResolvesServerCert for StaticCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.clone())
    }
}

struct LoadedState {
    key: Arc<CertifiedKey>,
    cert_mtime: Option<SystemTime>,
    key_mtime: Option<SystemTime>,
    last_check: Instant,
}

/// Certificate resolver that re-reads the certificate and key from disk when
/// their modification times change.
///
/// Files are checked at most once per `reload_interval`, during a TLS
/// handshake. If a reload fails (for example because only one of the two
/// files has been rewritten so far), the previous certificate keeps being
/// served and the reload is retried on the next check.
pub struct ReloadingCertResolver {
    cert_path: PathBuf,
    key_path: PathBuf,
    reload_interval: Duration,
    provider: Arc<CryptoProvider>,
    state: RwLock<LoadedState>,
}

impl fmt::Debug for ReloadingCertResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReloadingCertResolver")
            .field("cert_path", &self.cert_path)
            .field("key_path", &self.key_path)
            .field("reload_interval", &self.reload_interval)
            .finish_non_exhaustive()
    }
}

fn mtime(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path).and_then(|m| m.modified()).ok()
}

impl ReloadingCertResolver {
    /// Load the certificate and key, failing if they are missing or invalid.
    pub fn new(
        cert_path: impl Into<PathBuf>,
        key_path: impl Into<PathBuf>,
        reload_interval: Duration,
    ) -> Result<Self> {
        let cert_path = cert_path.into();
        let key_path = key_path.into();
        let provider = provider();
        let cert_mtime = mtime(&cert_path);
        let key_mtime = mtime(&key_path);
        let key = Arc::new(Self::load(&cert_path, &key_path, &provider)?);
        Ok(Self {
            cert_path,
            key_path,
            reload_interval,
            provider,
            state: RwLock::new(LoadedState {
                key,
                cert_mtime,
                key_mtime,
                last_check: Instant::now(),
            }),
        })
    }

    fn load(cert_path: &Path, key_path: &Path, provider: &CryptoProvider) -> Result<CertifiedKey> {
        let cert_pem = std::fs::read(cert_path)?;
        let key_pem = std::fs::read(key_path)?;
        certified_key_from_pem(&cert_pem, &key_pem, provider)
    }

    /// Reload from disk if the check interval has elapsed and a file changed.
    fn maybe_reload(&self) {
        {
            let state = self.state.read().unwrap_or_else(|e| e.into_inner());
            if state.last_check.elapsed() < self.reload_interval {
                return;
            }
        }
        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        // Another handshake may have done the check while we waited.
        if state.last_check.elapsed() < self.reload_interval {
            return;
        }
        state.last_check = Instant::now();

        let cert_mtime = mtime(&self.cert_path);
        let key_mtime = mtime(&self.key_path);
        if cert_mtime == state.cert_mtime && key_mtime == state.key_mtime {
            return;
        }
        match Self::load(&self.cert_path, &self.key_path, &self.provider) {
            Ok(key) => {
                state.key = Arc::new(key);
                state.cert_mtime = cert_mtime;
                state.key_mtime = key_mtime;
                info!(cert = %self.cert_path.display(), "Reloaded webhook TLS certificate");
            }
            Err(e) => warn!(
                error = %e,
                cert = %self.cert_path.display(),
                "Failed to reload webhook TLS certificate; keeping the previous one"
            ),
        }
    }

    /// The certificate currently being served.
    pub(crate) fn current(&self) -> Arc<CertifiedKey> {
        self.state
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .key
            .clone()
    }
}

impl ResolvesServerCert for ReloadingCertResolver {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.maybe_reload();
        Some(self.current())
    }
}

#[cfg(test)]
impl ReloadingCertResolver {
    /// Test hook: run a reload check now, regardless of the interval.
    pub(crate) fn force_check(&self) {
        {
            let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
            state.last_check = Instant::now()
                .checked_sub(self.reload_interval + Duration::from_millis(1))
                .unwrap_or(state.last_check);
        }
        self.maybe_reload();
    }
}
