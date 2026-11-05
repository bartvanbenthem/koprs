# apiconfigsync-operator

A Kubernetes operator written in Rust that manages a custom resource called `ApiConfigSync`. It polls an external HTTP API on each reconcile and stores the response body as a `ConfigMap` in a target namespace. A co-located validating admission webhook enforces spec correctness before any `ApiConfigSync` is persisted.

This example demonstrates using `koprs`, `koprs-external`, and `koprs-admission` together in a single operator binary.

## How it works

### Custom Resource: `ApiConfigSync`

You create an `ApiConfigSync` CR in any namespace, specifying:
- `targetNamespace` — the namespace where the ConfigMap will be created/maintained
- `apiUrl` — the HTTPS endpoint to poll for configuration data (must satisfy the [outbound URL policy](#outbound-url-policy-ssrf-protection))
- `bearerTokenSecretRef` *(optional)* — `{name, key}` of a Secret **in the CR's namespace** whose value is sent as `Authorization: Bearer <token>`. Tokens never live in the CR spec, where anyone with `get` on the resource could read them.
- `configKey` *(optional, default: `"config"`)* — the key name used in the resulting ConfigMap

Example:

```yaml
apiVersion: example.io/v1alpha1
kind: ApiConfigSync
metadata:
  name: app-config
  namespace: default
spec:
  targetNamespace: production
  apiUrl: https://config.example.com/api/v1/settings
  bearerTokenSecretRef:
    name: config-api-token
    key: token
  configKey: settings.json
---
apiVersion: v1
kind: Secret
metadata:
  name: config-api-token
  namespace: default
stringData:
  token: my-api-token
```

This creates a ConfigMap named `acs-default-app-config` (`acs-<cr-namespace>-<cr-name>`) in the `production` namespace with a single key `settings.json` whose value is the response body from the API endpoint.

### Reconcile loop

On each reconcile the operator:

1. **Adds a finalizer** to the CR to prevent deletion before cleanup runs.
2. **Re-checks the outbound URL policy** — the webhook enforces it too, but the webhook can be bypassed.
3. **Polls the external API** once per reconcile using `koprs-external`'s `HttpPoller` on a shared, hardened HTTP client (15s timeout, no redirects, SSRF-guarded DNS). Reads the Bearer token from the referenced Secret when configured. The response body (max 1 MiB) becomes the ConfigMap value.
4. **Applies the ConfigMap** (`acs-<cr-namespace>-<cr-name>`) in the target namespace using Server-Side Apply. The ConfigMap is labelled with `app.kubernetes.io/managed-by=apiconfigsync-operator` plus `apiconfigsync.example.io/owner` and `apiconfigsync.example.io/owner-namespace`.
5. **Garbage collects** stale ConfigMaps owned by **this** CR — the selector includes both owner labels, so one CR's GC never touches another CR's ConfigMaps.
6. **Stamps a label** — adds `apiconfigsync.example.io/synced-to=<target-namespace>` to the CR so the sync target is visible without reading the spec.
7. **Patches status** — writes `ready`, `message`, and a `Ready=True` condition (with `lastTransitionTime` and `observedGeneration`) in a single SSA patch.
8. **On deletion** — removes the synced ConfigMap, then removes its own finalizer (leaving any others in place) to allow the CR to be fully deleted.

Requeues every **300 seconds** for drift correction. Retries after **5 seconds** on error.

### Drift detection

The controller watches ConfigMaps carrying the `app.kubernetes.io/managed-by=apiconfigsync-operator` label. If one is modified externally, the owning CR is re-queued to restore the desired state. Because the ConfigMap lives in the *target* namespace, the owner is resolved from the `owner` + `owner-namespace` labels (`owner_labels_mapper`) rather than from the ConfigMap's own namespace.

### Outbound URL policy (SSRF protection)

Anyone allowed to create an `ApiConfigSync` makes the operator send an HTTP request, from inside the cluster, to a URL of their choosing — and copies the response into a ConfigMap they can read. Without restrictions that is a server-side request forgery (SSRF) hole: the URL could target the cloud metadata service (`169.254.169.254`), the Kubernetes API, or any in-cluster Service.

[`src/url_policy.rs`](src/url_policy.rs) applies two layers, enforced in both the webhook and the reconciler:

1. **URL checks** — `https://` only; no `user:password@`; IP-literal hosts must be public; internal names (`localhost`, `*.svc`, `*.cluster.local`, `*.internal`, single-label names) are refused; optional host allow-list.
2. **Guarded DNS** — the HTTP client's resolver drops every non-public address a hostname resolves to, which also defeats DNS rebinding. Redirects are disabled.

| Environment variable | Default | Effect |
|---|---|---|
| `APICONFIGSYNC_ALLOWED_HOSTS` | *(empty: any public host)* | Comma-separated hosts or `*.suffix` patterns |
| `APICONFIGSYNC_ALLOW_HTTP` | `false` | Permit plain `http://` URLs |
| `APICONFIGSYNC_ALLOW_PRIVATE_NETWORKS` | `false` | Permit private / in-cluster targets (e.g. a mock API Service in a dev cluster) |

Set an allow-list in production.

### Admission webhook

An admission webhook is registered at `POST /validate/apiconfigsync` and is invoked by Kubernetes before any `ApiConfigSync` is created or updated. It uses `koprs-admission`'s `WebhookBuilder` and `Validator` trait.

**Hard denials** (the resource is rejected):

| Rule | Error |
|------|-------|
| `spec.apiUrl` is empty | `spec.apiUrl must not be empty` |
| `spec.apiUrl` violates the outbound URL policy | e.g. `spec.apiUrl address 169.254.169.254 is not a public address` |
| `spec.bearerTokenSecretRef` has an empty `name` or `key` | `spec.bearerTokenSecretRef.name and .key must not be empty` |
| `spec.targetNamespace` is empty | `spec.targetNamespace must not be empty` |
| `spec.configKey` is empty | `spec.configKey must not be empty` |

**Non-blocking warnings** (the resource is accepted, but `kubectl` surfaces a message):

| Condition | Warning |
|-----------|---------|
| `spec.apiUrl` uses plain HTTP (only when `APICONFIGSYNC_ALLOW_HTTP=true`) | token and response travel unencrypted |
| `spec.bearerTokenSecretRef` is absent | polling will be unauthenticated |

The webhook server listens on port `8443`. When TLS certificate files are found at the paths set by `WEBHOOK_TLS_CERT` / `WEBHOOK_TLS_KEY` (default: `/tls/tls.crt`, `/tls/tls.key`) it serves HTTPS and re-reads the files automatically when cert-manager rotates them. When the files are absent it falls back to plain HTTP, which is useful for local development.

### Running controller and webhook concurrently

[`src/main.rs`](src/main.rs) drives both the controller loop and the webhook server on the same Tokio runtime:

```rust
tokio::try_join!(
    run_controller(client, operator_ns),
    run_webhook(),
)?;
```

`run_controller` builds a `ControllerBuilder` with health probes, Prometheus metrics, leader election, and a secondary ConfigMap watch. `run_webhook` builds a `WebhookBuilder` with the `ApiConfigSyncValidator` registered at `/validate/apiconfigsync`.

If either task returns an error, the other is cancelled and the process exits. In particular, when this replica loses its leader lease `run_controller` returns `LeaderElectionLost`, so the process exits non-zero and Kubernetes restarts the pod — instead of leaving a webhook-only zombie that never campaigns again.

### Status

```
kubectl get apiconfigsyncs
NAME         TARGET       APIURL                                          READY
app-config   production   https://config.example.com/api/v1/settings      true
```

## Prerequisites

- Kubernetes cluster (1.26+)
- `kubectl` configured to point at the cluster
- Rust toolchain (edition 2024) — only needed to build from source
- [cert-manager](https://cert-manager.io/) — issues the admission webhook's TLS certificate (in-cluster deployment only)
- Docker (or another OCI builder) — to build the operator image

## Deploy

All manifests live in [`manifests/`](manifests/):

| File | Contents |
|---|---|
| [`crd.yaml`](manifests/crd.yaml) | `ApiConfigSync` CRD, generated from `src/types.rs` (a unit test fails if it drifts) |
| [`rbac.yaml`](manifests/rbac.yaml) | `apiconfigsync-system` namespace, ServiceAccount, ClusterRole/Binding, leader-election Role/Binding |
| [`webhook.yaml`](manifests/webhook.yaml) | cert-manager Issuer + Certificate, webhook Service, `ValidatingWebhookConfiguration` (CA injected by cert-manager) |
| [`deployment.yaml`](manifests/deployment.yaml) | Two-replica operator Deployment (leader election, probes, `/tls` mount, URL-policy env) |
| [`example-cr.yaml`](manifests/example-cr.yaml) | Example `ApiConfigSync` and its bearer-token Secret |

### 1. Prerequisites in the cluster

[cert-manager](https://cert-manager.io/docs/installation/) issues the webhook's serving certificate and injects its CA into the webhook configuration:

```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml
kubectl -n cert-manager wait --for=condition=Available deploy --all --timeout=180s
```

### 2. Build the image

From the **workspace root** (the build needs the `koprs*` crates):

```bash
docker build -f examples/apiconfigsync/Dockerfile -t apiconfigsync-operator:dev .
kind load docker-image apiconfigsync-operator:dev   # when using kind; otherwise push to your registry
```

Using a registry? Change `image:` in `deployment.yaml` accordingly.

### 3. Install the operator

```bash
kubectl apply -f manifests/crd.yaml
kubectl apply -f manifests/rbac.yaml
kubectl apply -f manifests/webhook.yaml
kubectl apply -f manifests/deployment.yaml
kubectl -n apiconfigsync-system rollout status deploy/apiconfigsync-operator
```

Before going further, set `APICONFIGSYNC_ALLOWED_HOSTS` in `deployment.yaml` to the hosts your CRs may poll (see [Outbound URL policy](#outbound-url-policy-ssrf-protection)).

### 4. Create a CR

```bash
kubectl create namespace production
kubectl apply -f manifests/example-cr.yaml
kubectl get apiconfigsyncs
```

The webhook rejects CRs that break the URL policy, for example:

```bash
kubectl apply -f - <<'YAML'
apiVersion: example.io/v1alpha1
kind: ApiConfigSync
metadata: { name: ssrf-attempt, namespace: default }
spec:
  targetNamespace: production
  apiUrl: https://169.254.169.254/latest/meta-data/
YAML
# Error from server: admission webhook "validate.apiconfigsync.example.io" denied the request:
# spec.apiUrl address 169.254.169.254 is not a public address
```

### Running locally (out-of-cluster)

```bash
kubectl apply -f manifests/crd.yaml
RUST_LOG=info cargo run --release
```

The operator uses the kubeconfig from `~/.kube/config` (or the `KUBECONFIG` env var). With no certificate files at `/tls`, the webhook falls back to plain HTTP. The API server can't call it in that mode, so don't apply `webhook.yaml`; the reconciler still enforces the URL policy.

### RBAC

[`rbac.yaml`](manifests/rbac.yaml) grants:

| Resource | Verbs | Scope |
|---|---|---|
| `apiconfigsyncs` (example.io) | get, list, watch, patch, update | cluster |
| `apiconfigsyncs/status` (example.io) | get, patch, update | cluster |
| `configmaps` (core) | get, list, watch, create, update, patch, delete | cluster |
| `secrets` (core) | get — bearer-token Secrets; replace with per-namespace `Role`s to narrow it | cluster |
| `events` (events.k8s.io) | create, patch | cluster |
| `leases` (coordination.k8s.io) | get, create, update | `apiconfigsync-system` only |

## Project structure

```
src/
  main.rs        — wires controller and webhook; runs both with tokio::try_join!
  reconciler.rs  — ApiConfigSync reconciler; polls external API, manages ConfigMap
  url_policy.rs  — outbound URL policy + SSRF-guarded HTTP client
  validator.rs   — admission validator; enforces spec rules via koprs-admission
  types.rs       — ApiConfigSync CRD definition (+ test keeping manifests/crd.yaml in sync)
manifests/       — CRD, RBAC, webhook + cert-manager, Deployment, example CR
Dockerfile       — multi-stage build, run from the workspace root
Cargo.toml
```

## Dependencies

| Crate | Version | Purpose |
|---|---|---|
| `kube` | 4.2.0 | Kubernetes client + controller runtime |
| `k8s-openapi` | 0.28.0 (v1_36) | Typed Kubernetes API objects |
| `koprs` | workspace | Core operator abstractions (SSA, finalizers, status patching, GC, events) |
| `koprs-external` | workspace | HTTP polling via `HttpPoller` and `ExternalSource` |
| `koprs-admission` | workspace | Validating admission webhook server via `WebhookBuilder` and `Validator` |
| `tokio` | 1.0 | Async runtime |
| `tracing` / `tracing-subscriber` | 0.1 / 0.3 | Structured logging |

Log level is controlled via the `RUST_LOG` environment variable (default: `info`).
