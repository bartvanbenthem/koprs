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
- **Status patching** — patch the `/status` subresource of any CRD, cluster-scoped or namespaced
- **Finalizers** — add and remove finalizers on cluster-scoped and namespaced resources
- **Garbage collection** — diff-based GC for orphaned cluster and namespaced resources, with stuck-termination recovery
- **Watchers** — watch any resource type with optional label filtering, signal-based via `mpsc`
- **Listing** — list resources across namespaces or within a namespace, with or without label selectors
- **ObjectRefs** — build `ObjectRef` sets for cross-resource reconcile triggers
- **Persist to disk** — fetch a resource list and write it as JSON to a file
- **Typed errors** — `KubeGenericError` enum via `thiserror`, pattern-matchable by callers

---

## Installation

```toml
[dependencies]
koprs = { path = "../koprs" }
# or once published:
# koprs = "0.3.7"
```

---

## Module overview

| Module | Description |
|---|---|
| `resources` | Apply, delete, list, poll, and fetch resources (cluster + namespaced) |
| `status` | Patch `/status` subresource via SSA |
| `finalizers` | Add and remove finalizers |
| `gc` | Garbage collect orphaned resources |
| `watcher` | Watch resources for changes via `mpsc` signals |
| `scope` | `Cluster` and `Namespaced` scope markers for compile-time API selection |
| `traits` | `KubeResource`, `NamespacedResource`, `ClusterResource` trait aliases |
| `error` | `KubeGenericError` enum |

---

## Usage

### Apply a resource

The library exposes three layers of functions for applying resources:

| Function | Use when |
|---|---|
| `apply_namespaced_resource` | Your CRD is namespace-scoped (most common) |
| `apply_cluster_resource` | Your CRD is cluster-scoped |
| `apply_resource` | Scope is generic or passed through from a caller |

```rust
use koprs::resources::{
    apply_resource, apply_cluster_resource, apply_namespaced_resource,
};
use koprs::scope::{Cluster, Namespaced};

// Namespace-scoped CRD (convenience wrapper)
apply_namespaced_resource::(
    client.clone(),
    "my-namespace",
    &resource,
    "my-operator",
).await?;

// Cluster-scoped CRD (convenience wrapper)
apply_cluster_resource::(
    client.clone(),
    &resource,
    "my-operator",
).await?;

// Generic form — when scope is determined at runtime or passed through
apply_resource::(
    client.clone(),
    Namespaced("my-namespace"),
    &resource,
    "my-operator",
).await?;

apply_resource::(
    client.clone(),
    Cluster,
    &resource,
    "my-operator",
).await?;
```

### Delete a resource

The library exposes three layers of functions for deleting resources:

| Function | Use when |
|---|---|
| `delete_namespaced_resource` | Your CRD is namespace-scoped (most common) |
| `delete_cluster_resource` | Your CRD is cluster-scoped |
| `delete_resource` | Scope is generic or passed through from a caller |

```rust
use koprs::resources::{
    delete_resource, delete_cluster_resource, delete_namespaced_resource,
};
use koprs::scope::{Cluster, Namespaced};

// Namespace-scoped CRD (convenience wrapper)
// Returns Ok(false) if the resource did not exist
let deleted = delete_namespaced_resource::(
    client.clone(),
    "my-namespace",
    "my-resource",
).await?;

// Cluster-scoped CRD (convenience wrapper)
let deleted = delete_cluster_resource::(
    client.clone(),
    "my-resource",
).await?;

// Generic form — when scope is determined at runtime or passed through
let deleted = delete_resource::(
    client.clone(),
    Namespaced("my-namespace"),
    "my-resource",
).await?;

let deleted = delete_resource::(
    client.clone(),
    Cluster,
    "my-resource",
).await?;
```

### Ensure a namespace exists

```rust
use koprs::resources::ensure_namespace;

ensure_namespace(client.clone(), "my-namespace", "my-operator").await?;
```

### Patch status

The library exposes three layers of functions for patching status:

| Function | Use when |
|---|---|
| `patch_status_namespaced` | Your CRD is namespace-scoped (most common) |
| `patch_status_cluster` | Your CRD is cluster-scoped |
| `patch_status` | Scope is generic or passed through from a caller |

```rust
use koprs::status::{patch_status, patch_status_cluster, patch_status_namespaced};
use serde::Serialize;

#[derive(Serialize)]
struct MyStatus { ready: bool, message: String }

// Namespace-scoped CRD (convenience wrapper)
patch_status_namespaced::(
    client.clone(),
    "my-namespace",
    "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;

// Cluster-scoped CRD (convenience wrapper)
patch_status_cluster::(
    client.clone(),
    "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;

// Generic form — when scope is determined at runtime or passed through
use koprs::scope::{Cluster, Namespaced};

patch_status::(
    client.clone(),
    Namespaced("my-namespace"),
    "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;
```

### Finalizers

The library exposes three layers of functions for managing finalizers:

| Function | Use when |
|---|---|
| `add_finalizer_namespaced` / `remove_finalizers_namespaced` | Your CRD is namespace-scoped (most common) |
| `add_finalizer_cluster` / `remove_finalizers_cluster` | Your CRD is cluster-scoped |
| `add_finalizer` / `remove_finalizers` | Scope is generic or passed through from a caller |

```rust
use koprs::finalizers::{
    add_finalizer, add_finalizer_cluster, add_finalizer_namespaced,
    remove_finalizers, remove_finalizers_cluster, remove_finalizers_namespaced,
};

// Namespace-scoped CRD (convenience wrapper)
add_finalizer_namespaced::(
    client.clone(),
    "my-namespace",
    "my-resource",
    "my-operator/cleanup",
).await?;

remove_finalizers_namespaced::(
    client.clone(),
    "my-namespace",
    "my-resource",
).await?;

// Cluster-scoped CRD (convenience wrapper)
add_finalizer_cluster::(
    client.clone(),
    "my-resource",
    "my-operator/cleanup",
).await?;

remove_finalizers_cluster::(
    client.clone(),
    "my-resource",
).await?;

// Generic form — when scope is determined at runtime or passed through
use koprs::scope::{Cluster, Namespaced};

add_finalizer::(
    client.clone(),
    Namespaced("my-namespace"),
    "my-resource",
    "my-operator/cleanup",
).await?;

remove_finalizers::(
    client.clone(),
    Cluster,
    "my-resource",
).await?;
```

### Garbage collection

The library exposes three layers of functions for garbage collecting orphaned resources:

| Function | Use when |
|---|---|
| `gc_namespaced_resources` | Your CRD is namespace-scoped (most common) |
| `gc_cluster_resources` | Your CRD is cluster-scoped |
| `gc_resources` | Scope is generic, or you need a custom runtime scope |

All three variations accept a predicate closure (`Fn(&T) -> bool`) that evaluates whether a given resource should be kept. Any resource matching the label selector for which the predicate returns `false` will be garbage collected.

```rust
use std::collections::HashSet;
use kube::ResourceExt;
use koprs::gc::{gc_cluster_resources, gc_namespaced_resources, gc_resources};
use koprs::scope::{Cluster, Namespaced};

// Namespace-scoped CRD (convenience wrapper)
let desired = HashSet::from([
    ("default".to_string(), "my-resource-a".to_string()),
]);

gc_namespaced_resources::(
    client.clone(),
    "default",
    "app=my-operator",
    |r| {
        let ns = r.namespace().unwrap_or_default();
        desired.contains(&(ns, r.name_any()))
    },
).await?;

// Cluster-scoped CRD (convenience wrapper)
let desired = HashSet::from(["my-cluster-resource".to_string()]);

gc_cluster_resources::(
    client.clone(),
    "app=my-operator",
    |r| desired.contains(&r.name_any()),
).await?;

// Generic form — when scope is determined at runtime or passed through
let desired = HashSet::from([
    ("default".to_string(), "my-resource-b".to_string()),
]);

gc_resources::(
    client.clone(),
    Namespaced("default"),
    "app=my-operator",
    |r| {
        let ns = r.namespace().unwrap_or_default();
        desired.contains(&(ns, r.name_any()))
    },
).await?;
```

### Watcher

The library exposes three layers of functions for watching resources:

| Function | Use when |
|---|---|
| `watch_namespaced` / `watch_namespaced_by_label` | Your CRD is namespace-scoped (most common) |
| `watch_cluster` / `watch_cluster_by_label` | Your CRD is cluster-scoped |
| `watch` | Scope is generic or passed through from a caller |

```rust
use koprs::watcher::{
    watch, watch_cluster, watch_cluster_by_label,
    watch_namespaced, watch_namespaced_by_label,
};
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(16);

// Namespace-scoped CRD (convenience wrapper)
let _handle = watch_namespaced::(
    client.clone(),
    "my-namespace",
    tx.clone(),
).await?;

// Namespace-scoped CRD with label filter
let _handle = watch_namespaced_by_label::(
    client.clone(),
    "my-namespace",
    "app=my-operator",
    tx.clone(),
).await?;

// Cluster-scoped CRD (convenience wrapper)
let _handle = watch_cluster::(
    client.clone(),
    tx.clone(),
).await?;

// Cluster-scoped CRD with label filter
let _handle = watch_cluster_by_label::(
    client.clone(),
    "app=my-operator",
    tx.clone(),
).await?;

// Generic form — when scope is determined at runtime or passed through
use koprs::scope::{Cluster, Namespaced};

let _handle = watch::(
    client.clone(),
    Namespaced("my-namespace"),
    None,
    tx.clone(),
).await?;

let _handle = watch::(
    client.clone(),
    Cluster,
    Some("app=my-operator"),
    tx.clone(),
).await?;

// Consume signals from any of the above
while let Some(()) = rx.recv().await {
    println!("Resource changed!");
}
```

### List resources

```rust
use koprs::resources::{
    list_resources, list_resources_by_label, list_namespaced_resources, list_resource_names,
};
use k8s_openapi::api::core::v1::ConfigMap;

let all     = list_resources::(client.clone()).await?;
let labeled = list_resources_by_label::(client.clone(), "app=my-operator").await?;
let in_ns   = list_namespaced_resources::(client.clone(), "my-namespace").await?;
let names   = list_resource_names::(client.clone(), "app=my-operator").await?;
```

### Polling

The library exposes three layers of functions for polling until resources exist:

| Function | Use when |
|---|---|
| `wait_for_resources_namespaced` | Your CRD is namespace-scoped (most common) |
| `wait_for_resources_cluster` | Your CRD is cluster-scoped |
| `wait_for_resources` | Scope is generic or passed through from a caller |

```rust
use koprs::resources::{
    wait_for_resources, wait_for_resources_cluster, wait_for_resources_namespaced,
};
use std::time::Duration;

// Namespace-scoped CRD (convenience wrapper)
let resources = wait_for_resources_namespaced::(
    client.clone(),
    "my-namespace",
    Duration::from_secs(10),
).await?;

// Cluster-scoped CRD (convenience wrapper)
let resources = wait_for_resources_cluster::(
    client.clone(),
    Duration::from_secs(10),
).await?;

// Generic form — when scope is determined at runtime or passed through
use koprs::scope::{Cluster, Namespaced};

let resources = wait_for_resources::(
    client.clone(),
    Namespaced("my-namespace"),
    Duration::from_secs(10),
).await?;

let resources = wait_for_resources::(
    client.clone(),
    Cluster,
    Duration::from_secs(10),
).await?;

// Cardinality policy stays in your operator — the library returns Vec
match resources.len() {
    1 => { /* exactly one CR, proceed */ }
    n => { /* zero is unreachable here; handle n > 1 as your domain requires */ }
}
```

### ObjectRefs

```rust
use koprs::resources::{
    make_object_refs, make_object_refs_cluster, make_object_refs_namespaced,
    make_object_ref_mapper,
};
use koprs::scope::{Cluster, Namespaced};
use std::sync::Arc;

// Namespace-scoped (convenience wrapper)
let refs = make_object_refs_namespaced::(client.clone(), "my-namespace").await?;

// Cluster-scoped (convenience wrapper)
let refs = make_object_refs_cluster::(client.clone()).await?;

// Generic form — when scope is determined at runtime or passed through
let refs = make_object_refs::(client.clone(), Namespaced("my-namespace")).await?;
let refs = make_object_refs::(client.clone(), Cluster).await?;

// Build a mapper for cross-resource reconcile triggers
let mapper = make_object_ref_mapper::(Arc::new(refs));
```

### Persist to disk

```rust
use koprs::resources::fetch_and_write_to_file;
use k8s_openapi::api::core::v1::Pod;

fetch_and_write_to_file::(client.clone(), "/tmp", "pods.json").await?;
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
├── common.rs        # shared mock harness and fixture builders
├── resources.rs
├── status.rs
├── finalizers.rs
├── gc.rs
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

### CI script
`cargo-ci.sh` runs all quality checks in sequence — format, type-check,
unit tests, integration tests, coverage, release build, docs, and audit.

```bash
./scripts/cargo-ci.sh                           # run all steps
./scripts/cargo-ci.sh --fast                    # fmt + check + unit tests only (no coverage)
./scripts/cargo-ci.sh --no-audit                # skip cargo-audit
./scripts/cargo-ci.sh --no-integration          # skip integration tests
./scripts/cargo-ci.sh --no-doc                  # skip cargo doc
./scripts/cargo-ci.sh --no-coverage             # skip llvm-cov coverage report
./scripts/cargo-ci.sh --bench                   # also compile benchmarks (slow, opt-in)
./scripts/cargo-ci.sh --coverage-fail-under=80  # fail if line coverage drops below N%
```

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
