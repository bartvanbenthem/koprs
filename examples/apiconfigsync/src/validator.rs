// src/validator.rs
//
// Validating admission webhook for ApiConfigSync resources.
//
// Registered at POST /validate/apiconfigsync and invoked by Kubernetes before
// any ApiConfigSync is created or updated. The validator enforces the rules
// below and attaches non-blocking warnings for advisory conditions.
//
// Hard denials (HTTP 403 to the user):
//   - spec.apiUrl violates the operator's outbound URL policy (see
//     `url_policy.rs`): not https, embedded credentials, private / internal
//     target, or a host outside APICONFIGSYNC_ALLOWED_HOSTS
//   - spec.targetNamespace is empty
//   - spec.configKey is empty
//   - spec.bearerTokenSecretRef has an empty name or key
//
// Warnings (surfaced by kubectl, request still allowed):
//   - spec.apiUrl uses plain HTTP (only possible when APICONFIGSYNC_ALLOW_HTTP=true)
//   - spec.bearerTokenSecretRef is absent (polling will be unauthenticated)
//
// The reconciler re-checks the URL policy, so bypassing the webhook does not
// bypass the policy.

use koprs_admission::webhook::Validator;
use koprs_admission::{AdmissionRequest, ValidationResponse};

use crate::types::ApiConfigSync;
use crate::url_policy::UrlPolicy;

pub struct ApiConfigSyncValidator {
    pub policy: UrlPolicy,
}

impl Validator<ApiConfigSync> for ApiConfigSyncValidator {
    type Error = std::convert::Infallible;

    async fn validate(
        &self,
        request: &AdmissionRequest<ApiConfigSync>,
    ) -> Result<ValidationResponse, Self::Error> {
        // DELETE requests carry no object — nothing to validate.
        let Some(cr) = &request.object else {
            return Ok(ValidationResponse::allow());
        };

        let spec = &cr.spec;

        // --- Hard validation rules ---

        if spec.api_url.is_empty() {
            return Ok(ValidationResponse::deny("spec.apiUrl must not be empty"));
        }
        if let Err(reason) = self.policy.check(&spec.api_url) {
            return Ok(ValidationResponse::deny(reason));
        }
        if spec.target_namespace.is_empty() {
            return Ok(ValidationResponse::deny(
                "spec.targetNamespace must not be empty",
            ));
        }
        if spec.config_key.is_empty() {
            return Ok(ValidationResponse::deny("spec.configKey must not be empty"));
        }
        if let Some(r) = &spec.bearer_token_secret_ref
            && (r.name.is_empty() || r.key.is_empty())
        {
            return Ok(ValidationResponse::deny(
                "spec.bearerTokenSecretRef.name and .key must not be empty",
            ));
        }

        // --- Advisory warnings ---

        let mut warnings = Vec::new();

        if spec.api_url.starts_with("http://") {
            warnings.push(
                "spec.apiUrl uses plain HTTP; the bearer token and response are sent unencrypted"
                    .to_string(),
            );
        }
        if spec.bearer_token_secret_ref.is_none() {
            warnings.push(
                "spec.bearerTokenSecretRef is not set; the external API will be polled unauthenticated"
                    .to_string(),
            );
        }

        if warnings.is_empty() {
            Ok(ValidationResponse::allow())
        } else {
            Ok(ValidationResponse::allow_with_warnings(warnings))
        }
    }
}
