// src/url_policy.rs
//
// Outbound-request policy for `spec.apiUrl`.
//
// Anyone who can create an ApiConfigSync makes the operator issue an HTTP
// request to a URL of their choosing, from inside the cluster. Without a
// policy that is a server-side request forgery (SSRF) primitive: the URL
// could point at the cloud metadata endpoint (169.254.169.254), the
// Kubernetes API, or any in-cluster Service, and the response would be
// copied into a ConfigMap the requester can read.
//
// Two layers of defence:
//
// 1. `UrlPolicy::check` — static checks on the URL itself, enforced both by
//    the admission webhook (fast feedback) and again by the reconciler (the
//    webhook can be bypassed or misconfigured):
//      * scheme must be https (http only if explicitly allowed)
//      * no `user:password@` credentials in the URL
//      * IP-literal hosts must be publicly routable
//      * optional host allow-list (exact host or `*.suffix`)
//
// 2. `GuardedResolver` — a DNS resolver for the HTTP client that drops every
//    non-public address a hostname resolves to. This closes the gap a static
//    check cannot: a public-looking hostname that resolves (or later
//    re-resolves, "DNS rebinding") to a private address.
//
// Redirects are disabled on the client, so a permitted URL cannot bounce the
// request to a forbidden one.
//
// Configuration (environment variables on the operator pod):
//   APICONFIGSYNC_ALLOWED_HOSTS          comma-separated allow-list; empty = any public host
//   APICONFIGSYNC_ALLOW_HTTP             "true" to permit plain http:// URLs
//   APICONFIGSYNC_ALLOW_PRIVATE_NETWORKS "true" to permit private / in-cluster targets

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use reqwest::Url;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// Per-request timeout for calls to the external API.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
/// Largest response accepted — a ConfigMap cannot hold more than 1 MiB.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Default)]
pub struct UrlPolicy {
    /// Exact hostnames or `*.suffix` patterns. Empty means "any public host".
    pub allowed_hosts: Vec<String>,
    pub allow_http: bool,
    pub allow_private_networks: bool,
}

impl UrlPolicy {
    pub fn from_env() -> Self {
        let flag = |key: &str| {
            std::env::var(key)
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
        };
        Self {
            allowed_hosts: std::env::var("APICONFIGSYNC_ALLOWED_HOSTS")
                .unwrap_or_default()
                .split(',')
                .map(|h| h.trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .collect(),
            allow_http: flag("APICONFIGSYNC_ALLOW_HTTP"),
            allow_private_networks: flag("APICONFIGSYNC_ALLOW_PRIVATE_NETWORKS"),
        }
    }

    /// Validate `url` against the policy. The error is a user-facing message.
    pub fn check(&self, url: &str) -> Result<Url, String> {
        let parsed = Url::parse(url).map_err(|e| format!("spec.apiUrl is not a valid URL: {e}"))?;

        match parsed.scheme() {
            "https" => {}
            "http" if self.allow_http => {}
            "http" => {
                return Err(
                    "spec.apiUrl must use https:// (plain http is disabled on this operator)"
                        .into(),
                );
            }
            other => return Err(format!("spec.apiUrl scheme '{other}' is not allowed")),
        }

        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(
                "spec.apiUrl must not embed credentials; use spec.bearerTokenSecretRef".into(),
            );
        }

        let host = match parsed.host() {
            Some(url::Host::Domain(d)) => d.to_ascii_lowercase(),
            Some(url::Host::Ipv4(ip)) => {
                self.check_ip(IpAddr::V4(ip))?;
                ip.to_string()
            }
            Some(url::Host::Ipv6(ip)) => {
                self.check_ip(IpAddr::V6(ip))?;
                ip.to_string()
            }
            None => return Err("spec.apiUrl has no host".into()),
        };

        if !self.allow_private_networks && is_internal_hostname(&host) {
            return Err(format!(
                "spec.apiUrl host '{host}' is an internal name and is not allowed"
            ));
        }

        if !self.allowed_hosts.is_empty()
            && !self.allowed_hosts.iter().any(|p| host_matches(p, &host))
        {
            return Err(format!(
                "spec.apiUrl host '{host}' is not in the operator's allowed hosts"
            ));
        }

        Ok(parsed)
    }

    fn check_ip(&self, ip: IpAddr) -> Result<(), String> {
        if self.allow_private_networks || is_public(ip) {
            Ok(())
        } else {
            Err(format!("spec.apiUrl address {ip} is not a public address"))
        }
    }

    /// The HTTP client every reconcile shares: timeouts, no redirects, and a
    /// resolver that refuses non-public addresses.
    pub fn http_client(&self) -> reqwest::Client {
        let mut builder = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(Duration::from_secs(5))
            .redirect(reqwest::redirect::Policy::none());
        if !self.allow_private_networks {
            builder = builder.dns_resolver(Arc::new(GuardedResolver));
        }
        builder.build().expect("failed to build HTTP client")
    }
}

fn host_matches(pattern: &str, host: &str) -> bool {
    match pattern.strip_prefix("*.") {
        Some(suffix) => {
            host.len() > suffix.len() + 1
                && host.ends_with(suffix)
                && host.as_bytes()[host.len() - suffix.len() - 1] == b'.'
        }
        None => pattern == host,
    }
}

/// Names that only resolve inside the cluster or the host.
fn is_internal_hostname(host: &str) -> bool {
    let host = host.trim_end_matches('.');
    host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".local")
        || host.ends_with(".internal")
        || host.ends_with(".svc")
        || host.ends_with(".cluster.local")
        || !host.contains('.') // single-label names resolve via the cluster search path
}

/// `true` for globally routable unicast addresses.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => is_public_v4(v4),
            None => is_public_v6(v6),
        },
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local() // 169.254.0.0/16, incl. cloud metadata
        || ip.is_broadcast()
        || ip.is_multicast()
        || ip.is_documentation()
        || o[0] == 0 // 0.0.0.0/8
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10 carrier-grade NAT
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24 IETF protocol assignments
        || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18.0.0/15 benchmarking
        || o[0] >= 240) // reserved
}

fn is_public_v6(ip: Ipv6Addr) -> bool {
    let seg0 = ip.segments()[0];
    !(ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        || (seg0 & 0xfe00) == 0xfc00 // fc00::/7 unique local
        || (seg0 & 0xffc0) == 0xfe80 // fe80::/10 link local
        || seg0 == 0x2001 && ip.segments()[1] == 0x0db8) // 2001:db8::/32 documentation
}

/// DNS resolver that only returns publicly routable addresses.
#[derive(Debug)]
struct GuardedResolver;

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let resolved: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let public: Vec<SocketAddr> =
                resolved.into_iter().filter(|a| is_public(a.ip())).collect();
            if public.is_empty() {
                return Err(format!("'{host}' does not resolve to a public address").into());
            }
            Ok(Box::new(public.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn strict() -> UrlPolicy {
        UrlPolicy::default()
    }

    #[test]
    fn https_public_host_is_allowed() {
        assert!(strict().check("https://config.example.com/api").is_ok());
    }

    #[test]
    fn plain_http_is_rejected_unless_enabled() {
        assert!(strict().check("http://config.example.com/").is_err());
        let p = UrlPolicy {
            allow_http: true,
            ..strict()
        };
        assert!(p.check("http://config.example.com/").is_ok());
    }

    #[test]
    fn metadata_loopback_and_private_ips_are_rejected() {
        for url in [
            "https://169.254.169.254/latest/meta-data/",
            "https://127.0.0.1/",
            "https://10.0.0.1/",
            "https://192.168.1.1/",
            "https://100.64.0.1/",
            "https://[::1]/",
            "https://[fd00::1]/",
            "https://[::ffff:10.0.0.1]/",
        ] {
            assert!(strict().check(url).is_err(), "{url} should be rejected");
        }
    }

    #[test]
    fn internal_hostnames_are_rejected() {
        for url in [
            "https://kubernetes.default.svc/",
            "https://my-svc.my-ns.svc.cluster.local/",
            "https://localhost/",
            "https://kubernetes/",
            "https://metadata.google.internal/",
        ] {
            assert!(strict().check(url).is_err(), "{url} should be rejected");
        }
    }

    #[test]
    fn credentials_in_url_are_rejected() {
        assert!(
            strict()
                .check("https://user:pw@config.example.com/")
                .is_err()
        );
    }

    #[test]
    fn allow_list_matches_exact_and_wildcard_hosts() {
        let p = UrlPolicy {
            allowed_hosts: vec!["config.example.com".into(), "*.trusted.io".into()],
            ..strict()
        };
        assert!(p.check("https://config.example.com/").is_ok());
        assert!(p.check("https://api.trusted.io/").is_ok());
        assert!(p.check("https://trusted.io/").is_err());
        assert!(p.check("https://eviltrusted.io/").is_err());
        assert!(p.check("https://other.example.com/").is_err());
    }

    #[test]
    fn private_networks_can_be_enabled_explicitly() {
        let p = UrlPolicy {
            allow_private_networks: true,
            ..strict()
        };
        assert!(p.check("https://10.0.0.1/").is_ok());
        assert!(p.check("https://mock-api.default.svc/").is_ok());
    }

    #[tokio::test]
    async fn guarded_resolver_refuses_hosts_resolving_to_loopback() {
        let fut = GuardedResolver.resolve("localhost".parse().unwrap());
        assert!(fut.await.is_err());
    }
}
