# KOPRS - Kubernetes Operators Rust

A reusable, ergonomic library that streamlines Kubernetes operator development. By providing generic implementations for the most common operator patterns, it eliminates widespread boilerplate across your codebase. It integrates tightly with the `kube-rs` ecosystem to handle repetitive operational scaffolding, allowing developers to build reliable controllers with significantly less code.


## Architecture Overview

`koprs` is an opinionated, high-level orchestration framework built directly on top of `kube` and `kube-runtime`. While kube provides type-safe Kubernetes API bindings and kube-runtime delivers the controller primitives, koprs abstracts away the repetitive boilerplate required to build production ready controllers.

It encapsulates complex infrastructure orchestration loops, robust Server-Side Apply (SSA) patterns, and automated background garbage collection/cleanup processes out of your controller's core codebase. Additionally, it streamlines state synchronization with ready to use watcher logic and provides a strongly typed error handling model that removes the friction of building custom Kubernetes error variants from scratch. Every generic operation comes out of the box with structured, built-in `tracing` instrumentation, giving you deep visibility into your controller's execution paths without additional setup.

By lifting these structural requirements off your shoulders, koprs leaves you free to focus purely on your custom business logic.


```bash
+-------------------------------------------------------+
|                 Your Operator App                     |
|  (Business Logic, Sync Mode Matching, Storage Rules)  |
+-------------------------------------------------------+
                           |
                           v  [Turbofish Types Passed Down]
+-------------------------------------------------------+
|                    koprs                        |
|  (Generic SSA, Lifecycle Helpers, Status Patching)    |
+-------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------+
|                      kube-rs                          |
|         (Low-level Kubernetes API Engine)             |
+-------------------------------------------------------+
```

---

## Features

- **Apply & delete** — cluster-scoped and namespaced resources via Server-Side Apply (SSA)
- **Get** — fetch a single resource by name, returning `Option<T>` (`None` on 404)
- **Status patching** — patch the `/status` subresource of any CRD, cluster-scoped or namespaced
- **Finalizers** — add and remove finalizers on cluster-scoped and namespaced resources
- **Garbage collection** — diff-based GC for orphaned cluster and namespaced resources, with stuck-termination recovery
- **Watchers** — watch any resource type with optional label filtering, signal-based via `mpsc`
- **Listing** — list resources across namespaces or within a namespace, with or without label selectors
- **Ownership & controller wiring** — build `OwnerReference`s, set owner refs on children, generate `ObjectRef` sets, and create mapper closures for cross-resource reconcile triggers
- **Status conditions** — `make_condition` builds a `Condition` with the current timestamp; `upsert_condition` merges it with `lastTransitionTime` preservation. Include conditions in your status struct and patch them with `patch_status_*`
- **Patch labels / annotations** — merge labels or annotations onto any resource without replacing existing ones
- **Persist to disk** — fetch a resource list and write it as JSON to a file
- **Typed errors** — `KubeGenericError` enum via `thiserror`, pattern-matchable by callers

---

## Installation

```toml
[dependencies]
koprs = { path = "../koprs" }
# or once published:
# koprs = "<version>"
```

---

## Module overview

| Module | Description |
|---|---|
| `resources` | Apply, delete, get, list, poll, patch labels/annotations, and fetch resources |
| `status` | Patch `/status` subresource via SSA; `make_condition` and `upsert_condition` helpers |
| `finalizers` | Add and remove finalizers |
| `gc` | Garbage collect orphaned resources |
| `watcher` | Watch resources for changes via `mpsc` signals |
| `owners` | Owner references, child wiring, `ObjectRef` sets, and mapper closures |
| `scope` | `Cluster` and `Namespaced` scope markers for compile-time API selection |
| `traits` | `KubeResource`, `NamespacedResource`, `ClusterResource` trait aliases |
| `error` | `KubeGenericError` enum |

---

## Usage

Most operations come in three forms: `_namespaced` (most common), `_cluster`, and a generic scope form that accepts `Namespaced("ns")` or `Cluster`. The examples below show the namespaced form; the others follow the same signature.

### Apply and delete

```rust
use koprs::resources::{apply_namespaced_resource, delete_namespaced_resource};

apply_namespaced_resource::<MyCR>(client.clone(), "my-ns", &resource, "my-operator").await?;

// Returns Ok(false) if the resource was already gone
let deleted = delete_namespaced_resource::<MyCR>(client.clone(), "my-ns", "my-cr").await?;
```

### Finalizers

```rust
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizers_namespaced};

add_finalizer_namespaced::<MyCR>(client.clone(), "my-ns", "my-cr", "my-operator/cleanup").await?;
remove_finalizers_namespaced::<MyCR>(client.clone(), "my-ns", "my-cr").await?;
```

### Status

Include all status fields — scalars and conditions — in a single `patch_status_namespaced` call. Using separate patches with the same field manager causes each one to drop the fields from the other on every reconcile, producing an endless watch-event loop.

Preserve `lastTransitionTime` when the condition status has not changed so the patch is idempotent and does not bump `resourceVersion`.

```rust
use koprs::status::patch_status_namespaced;
use serde::Serialize;

#[derive(Serialize)]
struct MyStatus {
    ready: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    conditions: Vec<MyCondition>,
}

let last_transition_time = cr.status.as_ref()
    .and_then(|s| s.conditions.iter().find(|c| c.type_ == "Ready" && c.status == "True"))
    .map(|c| c.last_transition_time.clone())
    .unwrap_or_else(|| chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));

patch_status_namespaced::<MyCR, _>(
    client.clone(), "my-ns", "my-cr",
    MyStatus {
        ready: true,
        conditions: vec![MyCondition { type_: "Ready".into(), status: "True".into(), last_transition_time, .. }],
    },
    "my-operator",
).await?;
```

`make_condition` and `upsert_condition` from `koprs::status` are available as pure helpers when you need to build or merge `k8s_openapi::Condition` values before converting to your CRD's own condition type.

### Garbage collection

Accepts a keep-predicate: any resource matching the label selector for which the predicate returns `false` is deleted.

```rust
use koprs::gc::gc_namespaced_resources;

gc_namespaced_resources::<ConfigMap>(
    client.clone(), "my-ns", "app=my-operator",
    |r| desired_names.contains(&r.name_any()),
).await?;
```

### Watcher

```rust
use koprs::watcher::watch_namespaced_by_label;
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(16);
let _handle = watch_namespaced_by_label::<MyCR>(client.clone(), "my-ns", "app=my-operator", tx).await?;

while let Some(()) = rx.recv().await { /* resource changed */ }
```

### List and poll

```rust
use koprs::resources::{list_namespaced_resources, list_resource_names, wait_for_resources_namespaced};
use std::time::Duration;

let items = list_namespaced_resources::<MyCR>(client.clone(), "my-ns").await?;
let names = list_resource_names::<MyCR>(client.clone(), "app=my-operator").await?;
let items = wait_for_resources_namespaced::<MyCR>(client.clone(), "my-ns", Duration::from_secs(10)).await?;
```

### Ownership and controller wiring

```rust
use koprs::owners::{controller_ref, set_owner_refs, make_object_refs_namespaced, make_object_ref_mapper};
use std::sync::Arc;

let oref = controller_ref(&parent_cr)?;
set_owner_refs(&mut child, vec![oref]);

let refs   = make_object_refs_namespaced::<MyCR>(client.clone(), "my-ns").await?;
let mapper = make_object_ref_mapper::<TriggerType, _>(Arc::new(refs));
```

### Labels, annotations, and namespaces

```rust
use koprs::resources::{patch_labels_namespaced, patch_annotations_namespaced, ensure_namespace};

patch_labels_namespaced::<MyCR>(client.clone(), "my-ns", "my-cr", &[("app.kubernetes.io/managed-by", "my-operator")]).await?;
patch_annotations_namespaced::<MyCR>(client.clone(), "my-ns", "my-cr", &[("my-operator/synced", "true")]).await?;
ensure_namespace(client.clone(), "my-ns", "my-operator").await?;
```

---

### Error handling

All functions return `Result<T, KubeGenericError>`:

```rust
pub enum KubeGenericError {
    Kube(kube::Error),
    MissingMetadata(String),
    Serialization(serde_json::Error),
    Other(anyhow::Error),
}
```

`KubeGenericError` implements `std::error::Error` via `thiserror`, so it composes naturally with `anyhow` and the `?` operator. Variants are pattern-matchable for cases where you need to handle specific failures — for example, distinguishing a missing resource from a permission error:

```rust
use koprs::error::KubeGenericError;

match delete_cluster_resource::<Namespace>(client, "my-resource").await {
    Ok(true)  => info!("deleted"),
    Ok(false) => info!("already gone"),
    Err(KubeGenericError::Kube(kube::Error::Api(e))) if e.code == 403 => {
        error!("permission denied");
    }
    Err(e) => return Err(e.into()),
}
```

---

## Testing

### Unit tests
Unit tests use `tower_test::mock` to intercept HTTP requests and inject
hand-crafted JSON responses — no cluster or kubeconfig needed:
```bash
cargo test
```
Enable log output:
```bash
RUST_LOG=koprs=debug cargo test -- --nocapture
```
Tests are organised one file per module under `src/tests/`:
```
src/tests/
├── mod.rs
├── resources.rs
├── status.rs
├── finalizers.rs
├── gc.rs
├── owners.rs
├── watcher.rs
├── scope.rs
├── traits.rs
└── error.rs
```
To write your own tests, create a mock `(Client, Handle)` pair with
`tower_test::mock::pair` and serve responses from a background task:
```rust
use http::{Request, Response, StatusCode};
use kube::client::Body;
use kube::Client;
use serde_json::json;
use tower_test::mock;
type MockHandle = mock::Handle<Request<Body>, Response<Body>>;
fn mock_client() -> (Client, MockHandle) {
    let (svc, handle) = mock::pair::<Request<Body>, Response<Body>>();
    (Client::new(svc, "default"), handle)
}
#[tokio::test]
async fn my_test() {
    let (client, mut handle) = mock_client();
    // Serve the response in a background task — both sides must run
    // concurrently because the client blocks waiting for a response.
    let server = tokio::spawn(async move {
        let (req, send) = handle.next_request().await.unwrap();
        assert_eq!(req.method(), http::Method::GET);
        let body = serde_json::to_vec(&json!({
            "apiVersion": "v1",
            "kind": "ConfigMapList",
            "metadata": {},
            "items": []
        }))
        .unwrap();
        send.send_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        );
    });
    // Call the function under test using the mock client.
    let result = koprs::resources::list_resources::<k8s_openapi::api::core::v1::ConfigMap>(
        client,
    )
    .await
    .unwrap();
    assert!(result.items.is_empty());
    server.await.unwrap();
}
```
The mock handle serves requests in FIFO order. Functions that make multiple
API calls (such as the GC loop: list → delete → patch) require one
`handle.next_request()` call per request in the correct sequence.

---

### Integration tests
Integration tests run against a real cluster and are gated behind the
`integration` feature flag. The test functions are always compiled so type
errors are caught by `cargo check`, but they only execute when the feature
is enabled:
```bash
# Verify the integration tests compile without a cluster
cargo test --features integration --test integration --no-run
# Create a local cluster
kind create cluster --name koprs-test
# Run
cargo test --features integration --test integration
# Tear down
kind delete cluster --name koprs-test
```
Each test creates resources with a unique name suffix and cleans up after
itself, so the suite is safe to run with `--test-threads` greater than one.

---

## License

MIT
