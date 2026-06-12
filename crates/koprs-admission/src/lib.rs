//! # koprs-admission
//!
//! Validating admission webhook server for Kubernetes operators, designed as
//! a companion to [`koprs`](https://docs.rs/koprs).
//!
//! Kubernetes admission webhooks intercept API requests before they are
//! persisted and let operators enforce policy: reject resources that violate
//! naming conventions, block dangerous container configurations, or require
//! labels that your operator depends on. Writing the HTTP server, TLS wiring,
//! request parsing, and response serialisation for every webhook is
//! repetitive and error-prone. `koprs-admission` handles all of that.
//!
//! ## Core model
//!
//! Implement [`Validator`][webhook::Validator] for your resource type —
//! inspect the [`AdmissionRequest`] and return a [`ValidationResponse`].
//! Pass the validator to [`WebhookBuilder`][webhook::WebhookBuilder] and call
//! `.run()`. The framework handles the rest.
//!
//! ## Quick start
//!
//! ```no_run
//! use std::fs;
//! use koprs_admission::{AdmissionRequest, ValidationResponse};
//! use koprs_admission::webhook::{Validator, WebhookBuilder};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct MyResource { replicas: u32 }
//!
//! struct ReplicaLimit;
//!
//! impl Validator<MyResource> for ReplicaLimit {
//!     type Error = std::convert::Infallible;
//!
//!     async fn validate(
//!         &self,
//!         request: &AdmissionRequest<MyResource>,
//!     ) -> Result<ValidationResponse, Self::Error> {
//!         if request.object.as_ref().map_or(true, |r| r.replicas <= 10) {
//!             Ok(ValidationResponse::allow())
//!         } else {
//!             Ok(ValidationResponse::deny("replicas must not exceed 10"))
//!         }
//!     }
//! }
//!
//! # async fn example() -> Result<(), koprs_admission::AdmissionError> {
//! let cert_pem = fs::read("/tls/tls.crt")?;
//! let key_pem  = fs::read("/tls/tls.key")?;
//!
//! WebhookBuilder::new()
//!     .port(8443)
//!     .tls_from_pem(&cert_pem, &key_pem)?
//!     .health_port(8080)
//!     .graceful_shutdown()
//!     .validate("/validate/myresource", ReplicaLimit)
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod error;
pub mod review;
pub mod webhook;

pub use error::AdmissionError;
pub use review::{AdmissionRequest, Operation, ValidationResponse};
pub use webhook::{Validator, WebhookBuilder};

#[cfg(test)]
mod tests;
