# koprs-admission

Validating admission webhook server for Kubernetes operators, designed as a companion to [`koprs`](../koprs).

Kubernetes admission webhooks intercept API server requests before they are persisted, letting operators enforce policy: reject resources that violate naming conventions, block dangerous container configurations, or require labels that your operator depends on. Writing the HTTP server, TLS wiring, request parsing, and response serialisation for every webhook is repetitive and error-prone. `koprs-admission` handles all of that.

## Architecture overview

`koprs-admission` sits alongside your operator and exposes a typed webhook server over HTTPS. Kubernetes calls it for each matching resource operation; your `Validator` implementation inspects the request and returns an allow or deny decision.

```
+------------------------------------------------------+
|                 Your Operator App                    |
|  (Reconcile Kubernetes state + enforce admission)    |
+------------------------------------------------------+
          |                            |
          v                            v
+------------------+       +---------------------------+
|    koprs         |       |    koprs-admission        |
|  (controller,    |       |  (webhook server, TLS,    |
|   resources, gc) |       |   request parsing)        |
+------------------+       +---------------------------+
          |                            |
          v                            v
+------------------+       +---------------------------+
|    kube-rs       |       |  axum / rustls / hyper    |
+------------------+       +---------------------------+
```

---

## What koprs-admission provides

| Area | koprs-admission | rolling your own |
|---|---|---|
| **HTTP server** | `WebhookBuilder` binds the port, serves HTTPS, handles routing | You configure axum, hyper, and TLS manually |
| **TLS** | `.tls_from_pem()` loads cert-manager `Secret` volumes in one call | You wire up rustls, load PEM files, build `ServerConfig` |
| **Request parsing** | `AdmissionRequest<T>` — typed object, operation, uid, namespace | You parse `AdmissionReview` JSON and deserialise the raw `object` field |
| **Response building** | Return `ValidationResponse::allow()` or `ValidationResponse::deny("reason")` | You construct `AdmissionReview` response JSON with correct uid echo |
| **Validator errors** | `Err` from your validator returns HTTP 500, letting Kubernetes `failurePolicy` apply | You decide how to map errors to HTTP status codes |
| **Health probes** | `.health_port()` serves `GET /healthz` and `GET /readyz` | You wire up a second HTTP listener |
| **Graceful shutdown** | `.graceful_shutdown()` stops cleanly on SIGTERM / Ctrl+C | You handle signals and coordinate shutdown |
| **Multiple webhooks** | Chain `.validate()` calls to serve several resource types from one server | You manage multiple router routes and validator instances |

---

## Module overview

| Module | Description |
|---|---|
| `webhook` | `Validator` trait, `WebhookBuilder` |
| `review` | `AdmissionRequest<T>`, `ValidationResponse`, `Operation` |
| `error` | `AdmissionError` enum |

---

## Installation

```toml
[dependencies]
koprs-admission = { path = "../koprs-admission" }
# or once published:
# koprs-admission = "<version>"
```

---

## Usage

### Implementing a validator

`Validator<T>` is the only trait you implement. `T` is any type that derives `serde::Deserialize` — the framework parses the incoming `AdmissionReview` payload for you.

```rust
use koprs_admission::{AdmissionRequest, ValidationResponse};
use koprs_admission::webhook::Validator;
use serde::Deserialize;

#[derive(Deserialize)]
struct MyApp {
    image: String,
    replicas: u32,
}

struct PolicyValidator;

impl Validator<MyApp> for PolicyValidator {
    type Error = std::convert::Infallible;

    async fn validate(
        &self,
        request: &AdmissionRequest<MyApp>,
    ) -> Result<ValidationResponse, Self::Error> {
        let Some(app) = &request.object else {
            return Ok(ValidationResponse::allow());
        };

        if app.image.ends_with(":latest") {
            return Ok(ValidationResponse::deny(
                "image tag ':latest' is not allowed; pin to a specific digest or version tag",
            ));
        }

        if app.replicas > 10 {
            return Ok(ValidationResponse::deny(format!(
                "replicas {} exceeds the maximum of 10",
                app.replicas
            )));
        }

        Ok(ValidationResponse::allow())
    }
}
```

### Running the webhook server

```rust
use std::fs;
use koprs_admission::webhook::WebhookBuilder;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cert_pem = fs::read("/tls/tls.crt")?;
    let key_pem  = fs::read("/tls/tls.key")?;

    WebhookBuilder::new()
        .port(8443)
        .tls_from_pem(&cert_pem, &key_pem)?
        .health_port(8080)
        .graceful_shutdown()
        .validate("/validate/myapp", PolicyValidator)
        .run()
        .await?;

    Ok(())
}
```

### Multiple validators on one server

Chain `.validate()` calls to register multiple resource types on the same port. The path must match the `webhooks[].clientConfig.service.path` (or `url`) in your `ValidatingWebhookConfiguration`.

```rust
WebhookBuilder::new()
    .port(8443)
    .tls_from_pem(&cert_pem, &key_pem)?
    .validate("/validate/myapp", AppValidator)
    .validate("/validate/myconfig", ConfigValidator)
    .run()
    .await?;
```

### Returning warnings

Admit a resource but surface a non-blocking advisory message to the user:

```rust
Ok(ValidationResponse::allow_with_warnings(vec![
    "this image tag will be deprecated next quarter; migrate to v2".into(),
]))
```

Warnings appear in `kubectl apply` output even when the request is allowed.

### Handling validator errors

When your `validate` method returns `Err`, the webhook server responds with HTTP 500. Kubernetes then applies the `failurePolicy` configured on the `ValidatingWebhookConfiguration` (`Fail` or `Ignore`). Use this for transient errors (network calls, external lookups) that should be retried. For intentional denials, always return `Ok(ValidationResponse::deny(...))`.

### TLS — bringing your own `rustls::ServerConfig`

Use `.tls()` when you need full control over the TLS configuration, for example to enable client certificate authentication:

```rust
use std::sync::Arc;
use rustls::ServerConfig;

let config: Arc<ServerConfig> = todo!("build your ServerConfig");
WebhookBuilder::new()
    .port(8443)
    .tls(config)
    .run()
    .await?;
```

### Plain HTTP (TLS at the proxy)

Omit `.tls()` and `.tls_from_pem()` to run on plain HTTP. This is useful when TLS is terminated by a sidecar (Envoy, Istio) or a load balancer in front of the pod. Note that Kubernetes requires HTTPS for webhooks called by the API server directly — plain HTTP is only suitable when a TLS-terminating proxy sits between the API server and your pod.

---

## Kubernetes webhook configuration

Create a `ValidatingWebhookConfiguration` pointing to your service. The `clientConfig.service.path` must match the path you pass to `.validate()`.

```yaml
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: my-operator-webhook
webhooks:
  - name: myapp.example.io
    admissionReviewVersions: ["v1"]
    sideEffects: None
    rules:
      - apiGroups: ["example.io"]
        apiVersions: ["v1alpha1"]
        resources: ["myapps"]
        operations: ["CREATE", "UPDATE"]
    clientConfig:
      service:
        name: my-operator-webhook
        namespace: my-operator-system
        port: 8443
        path: /validate/myapp
      caBundle: <base64-encoded CA certificate>
    failurePolicy: Fail
    timeoutSeconds: 10
```

For TLS certificate issuance, [cert-manager](https://cert-manager.io/) with a `Certificate` resource is the standard approach. Mount the resulting `Secret` into the pod at `/tls/tls.crt` and `/tls/tls.key`.

---

## Testing

### Unit tests

Unit tests exercise validator logic, request parsing, and response building entirely in-process — no HTTP server or TLS required. Validators are called directly and HTTP round-trips use `tower::ServiceExt::oneshot` on the axum router.

```bash
cargo test -p koprs-admission
```

### Integration tests

Integration tests spin up a plain-HTTP webhook server on a random port and exercise the full request/response path with real HTTP calls. No Kubernetes cluster required.

```bash
cargo test -p koprs-admission --features integration --test integration
```

### Testing your own `Validator`

Because `Validator::validate` takes a plain `&AdmissionRequest<T>`, you can unit test it without any HTTP machinery:

```rust
#[tokio::test]
async fn deny_latest_tag() {
    let req = AdmissionRequest {
        uid: "test".into(),
        name: "my-app".into(),
        namespace: Some("default".into()),
        operation: Operation::Create,
        object: Some(MyApp { image: "nginx:latest".into(), replicas: 1 }),
        old_object: None,
        dry_run: false,
    };
    let resp = PolicyValidator.validate(&req).await.unwrap();
    assert!(!resp.allowed);
    assert!(resp.message.as_deref().unwrap().contains("':latest'"));
}
```
