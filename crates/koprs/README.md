# KOPRS - Kubernetes Operators Rust

A reusable, ergonomic library that eliminates Kubernetes operator boilerplate by providing generic implementations of the most common patterns on top of `kube-rs`.


## Architecture Overview

`koprs` is an opinionated, high-level orchestration framework built directly on top of `kube` and `kube-runtime`. It encapsulates SSA patterns, controller lifecycle management, garbage collection, watcher logic, and a strongly typed error model, all with built-in `tracing` instrumentation, so you can focus purely on your custom business logic.


```bash
+-------------------------------------------------------+
|                 Your Operator App                     |
|  (Business Logic, Sync Mode Matching, Storage Rules)  |
+-------------------------------------------------------+
                           |
                           v  [Turbofish Types Passed Down]
+-------------------------------------------------------+
|                    koprs                              |
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

## What koprs adds over plain kube-rs

| Area | koprs | plain kube / kube-runtime |
|---|---|---|
| **Controller bootstrap** | `ControllerBuilder` — health probes, leader election, graceful shutdown, timeouts, concurrency, secondary watches, all composable | Raw `Controller::new().run(...)` stream, no operational skeleton |
| **Apply / ensure** | `apply_resource`, `ensure_resource` (SSA), `EnsureOutcome<T>` (Created / Updated / Unchanged) | `api.patch()` — caller builds every `PatchParams` and branches on 404 manually |
| **Status patching** | `patch_status_namespaced` / cluster variants; `KoprsCondition` derives `JsonSchema` for direct CRD embedding | `api.patch_status()` exists; no ready-made condition type with `JsonSchema` |
| **Finalizers** | `add_finalizer` / `remove_finalizers` — idempotent merge-patch, no-op if already present/absent | No helpers; callers patch the finalizer list themselves |
| **Garbage collection** | `gc_resources` — list by label selector, delete orphans, clear finalizers on stuck-terminating resources | Not provided |
| **Event recording** | `record_event` with `EventType::Normal` / `Warning` | Not provided |
| **Owner references** | `owner_ref`, `controller_ref`, `set_owner_refs`; `make_object_ref_mapper`, `owner_label_mapper` for cross-resource reconcile triggers | `OwnerReference` struct exists; no builder or mapper helpers |
| **Scope markers** | `Cluster` / `Namespaced` compile-time markers resolve to the right `Api` constructor | Callers choose `Api::all` vs `Api::namespaced` at every call site |
| **Metadata builder** | Fluent `ObjectMetaBuilder` | `ObjectMeta { name: Some(...), labels: Some(BTreeMap::from([...])), ..Default::default() }` |
| **Watcher abstraction** | `watch` (signal), `watch_objects` (resource data), `watch_events` (applied + deleted) — mpsc channels, backoff, tracing included | Raw `watcher()` stream; callers wire mpsc, backoff, and error handling themselves |
| **Generic bounds** | `KubeResource` blanket trait collapses `Clone + Debug + Resource<DynamicType=()> + DeserializeOwned + Serialize + Send + Sync + 'static` to one name | Full bound wall on every generic function |
| **Error type** | `KubeGenericError` unifies `kube::Error`, `serde_json::Error`, `io::Error`, and internal errors | Each operator defines its own error type |

---

## Features

### Controller framework
- **`Reconciler` trait** — implement `reconcile` for your CRD; `error_policy` defaults to requeue after 30 s
- **`ControllerBuilder`** — one fluent builder that wires the reconcile loop; all methods are optional and composable:

| Method | What it provides |
|--------|-----------------|
| `.health_port(port)` | `GET /healthz` (liveness) + `GET /readyz` (readiness) HTTP server |
| `.graceful_shutdown()` | Clean stop on SIGTERM or Ctrl+C |
| `.leader_election(ns, name)` | Kubernetes Lease-based HA — only one replica reconciles at a time |
| `.leader_election_timings(dur, renew, retry)` | Override lease duration, renew period, and retry period (call after `.leader_election()`) |
| `.reconcile_timeout(dur)` | Cancel and requeue reconciles that exceed the duration |
| `.concurrency(n)` | Cap concurrent reconciles across all objects (default: unbounded; a single object is never reconciled concurrently regardless) |
| `.watch(api, config, mapper)` | Trigger re-queues from a secondary resource via a mapper function; calls compose |
| `.owns(api, config)` | Trigger re-queues from a child resource via Kubernetes owner references |
| `.with_watches(fn)` | Raw `kube_runtime::Controller` access for advanced watch configuration |
| `.label_selector(selector)` | Filter the primary resource watch by label |
| `.watcher_config(config)` | Replace the default watcher configuration for the primary watch |

```rust
use std::time::Duration;
use k8s_openapi::api::apps::v1::Deployment;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::{Api, Client};
use koprs::controller::{Context, ControllerBuilder, watcher};
use koprs::owners::owner_label_mapper;

let client = Client::try_default().await?;
let ctx = Context::new(client.clone());

ControllerBuilder::new(Api::<MyCR>::all(client.clone()))
    // re-queue the CR whenever a managed Deployment changes (owner reference)
    .owns(
        Api::<Deployment>::all(client.clone()),
        watcher::Config::default(),
    )
    // re-queue the CR whenever a managed ConfigMap carrying the owner label changes
    .watch(
        Api::<ConfigMap>::all(client.clone()),
        watcher::Config::default().labels("app.kubernetes.io/managed-by=my-operator"),
        owner_label_mapper("my-operator/owner"),
    )
    .health_port(8080)
    .graceful_shutdown()
    .leader_election("my-namespace", "my-operator-leader")
    .leader_election_timings(
        Duration::from_secs(15), // lease duration
        Duration::from_secs(5),  // renew period
        Duration::from_secs(2),  // retry period
    )
    .reconcile_timeout(Duration::from_secs(120))
    .concurrency(4)              // at most 4 objects reconciled simultaneously
    .run(MyReconciler, ctx)
    .await?;
```

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
| `status` | Patch `/status` subresource via SSA; `KoprsCondition` type, `make_condition` and `upsert_condition` helpers |
| `meta` | `ObjectMetaBuilder` — fluent builder for `ObjectMeta` |
| `finalizers` | Add and remove finalizers |
| `gc` | Garbage collect orphaned resources |
| `watcher` | `watch` (signal), `watch_objects` (resource data), `watch_events` (applied + deleted); `WatchEvent<T>` type |
| `owners` | Owner references, child wiring, `ObjectRef` sets, `owner_label_mapper`, and mapper closures |
| `scope` | `Cluster` and `Namespaced` scope markers for compile-time API selection |
| `traits` | `KubeResource`, `NamespacedResource`, `ClusterResource` trait aliases; `is_being_deleted` helper |
| `error` | `KubeGenericError` enum |

---

## Usage

Every operation takes an explicit scope argument — either `Namespaced("ns")` for namespace-scoped resources or `Cluster` for cluster-scoped ones. The scope is passed at the call site rather than encoded in the function name, so the routing is always visible.

### Apply and delete

```rust
use koprs::resources::{apply_resource, delete_resource};
use koprs::scope::{Cluster, Namespaced};

// Namespaced resource
apply_resource::<MyCR, _>(client.clone(), Namespaced("my-ns"), &resource, "my-operator").await?;

// Returns Ok(false) if the resource was already gone
let deleted = delete_resource::<MyCR, _>(client.clone(), Namespaced("my-ns"), "my-cr").await?;

// Cluster-scoped resource — same function, different scope marker
let deleted = delete_resource::<MyClusterCR, _>(client.clone(), Cluster, "my-cr").await?;
```

### Finalizers

```rust
use koprs::finalizers::{add_finalizer_namespaced, remove_finalizers};
use koprs::scope::Namespaced;

// add_finalizer_namespaced extracts the namespace from the resource metadata —
// no-op if the finalizer is already present, safe to call on every reconcile.
add_finalizer_namespaced::<MyCR>(client.clone(), &cr, "my-operator/cleanup").await?;

// Removing finalizers uses the generic scope form — pass Cluster for cluster-scoped resources.
remove_finalizers::<MyCR, _>(client.clone(), Namespaced("my-ns"), "my-cr").await?;
```

### Status

`KoprsCondition` derives `JsonSchema` so it can be embedded directly in a `CustomResource`-derived status struct — no mirror type or manual conversions required.

Include all status fields — scalars and conditions — in a single `patch_status_namespaced` call. Using separate patches with the same field manager causes each one to drop the other's fields on every reconcile, producing an endless watch-event loop.

`upsert_condition` preserves `lastTransitionTime` when the condition status has not changed, so the patch is idempotent and does not bump `resourceVersion` unnecessarily.

```rust
use koprs::status::{KoprsCondition, make_condition, patch_status_namespaced, upsert_condition};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// KoprsCondition is used directly — no mirror type needed.
#[derive(Serialize, Deserialize, JsonSchema)]
struct MyStatus {
    ready: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    conditions: Vec<KoprsCondition>,
}

let mut conditions = cr.status.as_ref()
    .map(|s| s.conditions.clone())
    .unwrap_or_default();

upsert_condition(
    &mut conditions,
    make_condition("Ready", "True", "Synced", "All good", cr.metadata.generation),
);

patch_status_namespaced::<MyCR, _>(
    client.clone(), "my-ns", "my-cr",
    MyStatus { ready: true, conditions },
    "my-operator",
).await?;
```

### Metadata builder

`ObjectMetaBuilder` replaces the verbose `ObjectMeta { name: Some(...), labels: Some(BTreeMap::from([...])), ..Default::default() }` construction pattern:

```rust
use koprs::meta::ObjectMetaBuilder;

let meta = ObjectMetaBuilder::new()
    .name("my-configmap")
    .namespace("my-ns")
    .label("app.kubernetes.io/managed-by", "my-operator")
    .label("my-operator/owner", "my-cr")
    .build();
```

### Deletion guard

Use `is_being_deleted` at the top of the reconcile loop to branch into the cleanup path:

```rust
use koprs::is_being_deleted;
use koprs::finalizers::remove_finalizers;
use koprs::scope::Namespaced;

if is_being_deleted(&*cr) {
    // clean up owned resources, then remove finalizer
    remove_finalizers::<MyCR, _>(client.clone(), Namespaced(&namespace), &name).await?;
    return Ok(Action::await_change());
}
```

### Garbage collection

Accepts a keep-predicate: any resource matching the label selector for which the predicate returns `false` is deleted.

```rust
use koprs::gc::gc_resources;
use koprs::scope::{Cluster, Namespaced};

// Namespaced
gc_resources::<ConfigMap, _>(
    client.clone(), Namespaced("my-ns"), "app=my-operator",
    |r| desired_names.contains(&r.name_any()),
).await?;

// Cluster-scoped — same function, Cluster scope
gc_resources::<MyClusterCR, _>(
    client.clone(), Cluster, "app=my-operator",
    |r| desired_names.contains(&r.name_any()),
).await?;
```

### Watcher

Three functions cover progressively richer data, all sharing the same scope + optional label-selector signature:

#### Signal only — `watch`

Cheapest option. Sends `()` on every ADDED or MODIFIED event. Use this to re-queue a reconcile when a child resource changes.

```rust
use koprs::watcher::watch;
use koprs::scope::Namespaced;
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(16);
let _handle = watch::<MyCR, _>(client.clone(), Namespaced("my-ns"), Some("app=my-operator"), tx).await?;

while let Some(()) = rx.recv().await { /* re-queue work */ }
```

#### Resource data on applies — `watch_objects`

Sends the full resource `T` on every ADDED or MODIFIED event. Use this to maintain a local cache without a follow-up GET. Deletions are not reported.

```rust
use koprs::watcher::watch_objects;
use koprs::scope::Namespaced;
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(16);
let _handle = watch_objects::<MyCR, _>(client.clone(), Namespaced("my-ns"), None, tx).await?;

while let Some(resource) = rx.recv().await {
    // resource is a fully populated MyCR — no extra GET needed
}
```

#### Full event model — `watch_events`

Sends `WatchEvent<T>` for every event, including deletions. Objects observed during a watch restart arrive as `Applied` so the stream is always consistent.

```rust
use koprs::watcher::{watch_events, WatchEvent};
use koprs::scope::{Cluster, Namespaced};
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(16);

// Namespaced with label filter
let _handle = watch_events::<MyCR, _>(client.clone(), Namespaced("my-ns"), Some("app=my-operator"), tx).await?;

// Cluster-scoped, no filter
let _handle = watch_events::<MyClusterCR, _>(client.clone(), Cluster, None, tx).await?;

while let Some(event) = rx.recv().await {
    match event {
        WatchEvent::Applied(r) => { /* created or modified */ }
        WatchEvent::Deleted(r) => { /* deleted — note: events may be missed if
                                       the watcher was down; use finalizers for
                                       guaranteed cleanup */ }
    }
}
```

### List and poll

```rust
use koprs::resources::{list_resources_scoped, list_resource_names, wait_for_resources};
use koprs::scope::{Cluster, Namespaced};
use kube::api::ListParams;
use std::time::Duration;

// List in a namespace with a label filter
let items = list_resources_scoped::<MyCR, _>(
    client.clone(),
    Namespaced("my-ns"),
    ListParams::default().labels("app=my-operator"),
).await?;

// List across all namespaces (or cluster-scoped resources) with a field filter
let items = list_resources_scoped::<MyCR, _>(
    client.clone(),
    Cluster,
    ListParams::default().fields("status.phase=Running"),
).await?;

// Names only — useful for GC diffing
let names = list_resource_names::<MyCR>(client.clone(), "app=my-operator").await?;

// Poll until at least one resource exists
let items = wait_for_resources::<MyCR, _>(
    client.clone(), Namespaced("my-ns"), Duration::from_secs(10),
).await?;
```

### Cross-resource watches and ownership

#### `.watch()` — secondary trigger wiring

Use `.watch()` on `ControllerBuilder` to re-queue a CR whenever a secondary resource changes. Multiple calls compose — all watches are active simultaneously.

`owner_label_mapper` covers the most common pattern: the trigger resource carries a label whose *value* is the name of the CR to re-queue, and its namespace is where the CR lives.

```rust
use koprs::controller::{ControllerBuilder, watcher};
use koprs::owners::owner_label_mapper;

ControllerBuilder::new(primary_api)
    // Re-queue owning CR when a managed ConfigMap changes.
    .watch(
        cm_api,
        watcher::Config::default().labels("app=my-operator"),
        owner_label_mapper("my-operator/owner"),
    )
    // Chain a second watch for a different resource type — both are active.
    .watch(
        secret_api,
        watcher::Config::default().labels("app=my-operator"),
        owner_label_mapper("my-operator/owner"),
    )
    // Use .with_watches() for full kube-runtime Controller access when needed.
    .with_watches(|ctl| ctl.owns(/* ... */))
    .run(MyReconciler, ctx)
    .await?;
```

#### Owner references

```rust
use koprs::owners::{controller_ref, set_owner_refs, make_object_refs, make_object_ref_mapper};
use koprs::scope::Namespaced;
use std::sync::Arc;

let oref = controller_ref(&parent_cr)?;
set_owner_refs(&mut child, vec![oref]);

let refs   = make_object_refs::<MyCR, _>(client.clone(), Namespaced("my-ns")).await?;
let mapper = make_object_ref_mapper::<TriggerType, _>(Arc::new(refs));
```

### Labels, annotations, and namespaces

```rust
use koprs::resources::{patch_labels, patch_annotations, ensure_namespace};
use koprs::scope::Namespaced;

patch_labels::<MyCR, _>(client.clone(), Namespaced("my-ns"), "my-cr", &[("app.kubernetes.io/managed-by", "my-operator")]).await?;
patch_annotations::<MyCR, _>(client.clone(), Namespaced("my-ns"), "my-cr", &[("my-operator/synced", "true")]).await?;
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
    Io(std::io::Error),
    Internal(String),
}
```

`KubeGenericError` implements `std::error::Error` via `thiserror` and composes with the `?` operator. Variants are pattern-matchable for cases where you need to handle specific failures — for example, distinguishing a missing resource from a permission error:

```rust
use koprs::error::KubeGenericError;
use koprs::resources::delete_resource;
use koprs::scope::Cluster;

match delete_resource::<Namespace, _>(client, Cluster, "my-resource").await {
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
├── meta.rs
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
    let result = koprs::resources::list_resources_scoped::<k8s_openapi::api::core::v1::ConfigMap, _>(
        client,
        koprs::scope::Cluster,
        Default::default(),
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
