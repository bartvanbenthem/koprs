# Kube Generic Operations

A reusable library of generic, ergonomic functions for the most common Kubernetes operator patterns. Built on top of [`kube`](https://docs.rs/kube) and [`kube-runtime`](https://docs.rs/kube-runtime), it eliminates boilerplate across operators by providing a single, well tested implementation for each pattern.


## Architecture Overview

`kube-genops` acts as a middle tier between your controller's core domain logic and the low-level Kubernetes API engine. By moving infrastructure orchestration loops, generic Server-Side Apply (SSA) patterns, and background janitors out of your application binaries, you can focus purely on your business logic.

```bash
+-------------------------------------------------------+
|                 Your Operator App                     |
|  (Business Logic, Sync Mode Matching, Storage Rules)  |
+-------------------------------------------------------+
                           |
                           v  [Turbofish Types Passed Down]
+-------------------------------------------------------+
|                    kube_genops                        |
|  (Generic SSA, Janitors, GC Loops, Status Patching)   |
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
kube-genops = { path = "../kube-genops" }
# or once published:
# kube-genops = "0.1"
```

---

## Module overview

| Module | Description |
|---|---|
| `resources` | Apply, delete, list, fetch resources (cluster + namespaced) |
| `status` | Patch `/status` subresource via SSA |
| `finalizers` | Add and remove finalizers |
| `gc` | Garbage collect orphaned resources |
| `watcher` | Watch resources for changes via `mpsc` signals |
| `error` | `KubeGenericError` enum |

---

## Usage

### Apply a resource

```rust
use kube::Client;
use kube_genops::resources::{apply_cluster_resource, apply_namespaced_resource};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolume};

// Cluster-scoped
apply_cluster_resource(client.clone(), &pv, "my-operator").await?;

// Namespaced
apply_namespaced_resource(client.clone(), "my-namespace", &cm, "my-operator").await?;
```

### Delete a resource

```rust
use kube_genops::resources::{delete_cluster_resource, delete_namespaced_resource};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolume};

// Returns Ok(false) if the resource did not exist
let deleted = delete_cluster_resource::<PersistentVolume>(client.clone(), "my-pv").await?;
let deleted = delete_namespaced_resource::<ConfigMap>(client.clone(), "my-ns", "my-cm").await?;
```

### Patch status

The library exposes three functions depending on how much flexibility you need:

| Function | Use when |
|---|---|
| `patch_status_namespaced` | Your CRD is namespace-scoped (most common) |
| `patch_status_cluster` | Your CRD is cluster-scoped |
| `patch_status` | Scope is generic or passed through from a caller |

```rust
use kube_genops::status::{patch_status, patch_status_cluster, patch_status_namespaced};
use serde::Serialize;

#[derive(Serialize)]
struct MyStatus { ready: bool, message: String }

// Namespace-scoped CRD (convenience wrapper)
patch_status_namespaced::<MyCR, _>(
    client.clone(),
    "my-namespace",
    "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;

// Cluster-scoped CRD (convenience wrapper)
patch_status_cluster::<MyCR, _>(
    client.clone(),
    "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;

// Generic form — when scope is determined at runtime or passed through
use kube_genops::scope::{Cluster, Namespaced};

patch_status::<MyCR, _, _>(
    client.clone(), Namespaced("my-namespace"), "my-resource",
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
use kube_genops::finalizers::{
    add_finalizer, add_finalizer_cluster, add_finalizer_namespaced,
    remove_finalizers, remove_finalizers_cluster, remove_finalizers_namespaced,
};

// Namespace-scoped CRD (convenience wrapper)
add_finalizer_namespaced::<MyCR>(
    client.clone(),
    "my-namespace",
    "my-resource",
    "my-operator/cleanup",
).await?;

remove_finalizers_namespaced::<MyCR>(
    client.clone(),
    "my-namespace",
    "my-resource",
).await?;

// Cluster-scoped CRD (convenience wrapper)
add_finalizer_cluster::<MyCR>(
    client.clone(),
    "my-resource",
    "my-operator/cleanup",
).await?;

remove_finalizers_cluster::<MyCR>(
    client.clone(),
    "my-resource",
).await?;

// Generic form — when scope is determined at runtime or passed through
use kube_genops::scope::{Cluster, Namespaced};

add_finalizer::<MyCR, _>(
    client.clone(),
    Namespaced("my-namespace"),
    "my-resource",
    "my-operator/cleanup",
).await?;

remove_finalizers::<MyCR, _>(
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
| `gc_resources` | Scope is generic, or you need a custom predicate |

The scoped wrappers take a `HashSet` of desired names. The generic form takes
a predicate `Fn(&T) -> bool` instead, which lets you express any desired-set
shape — including the `(namespace, name)` pairs needed for namespaced resources.

```rust
use std::collections::HashSet;
use kube::ResourceExt;
use kube_genops::gc::{gc_cluster_resources, gc_namespaced_resources, gc_resources};
use kube_genops::scope::{Cluster, Namespaced};

// Namespace-scoped CRD (convenience wrapper)
let desired = HashSet::from([
    ("default".to_string(), "my-resource".to_string()),
]);
gc_namespaced_resources::<MyCR>(
    client.clone(),
    "app=my-operator",
    &desired,
).await?;

// Cluster-scoped CRD (convenience wrapper)
let desired = HashSet::from(["my-resource".to_string()]);
gc_cluster_resources::<MyCR>(
    client.clone(),
    "app=my-operator",
    &desired,
).await?;

// Generic form — custom predicate, scope determined at runtime
let desired: HashSet<(String, String)> = HashSet::from([
    ("default".to_string(), "my-resource".to_string()),
]);
gc_resources::<MyCR, _>(
    client.clone(),
    Namespaced("default"),
    "app=my-operator",
    |r| {
        let ns = r.namespace().unwrap_or_default();
        desired.contains(&(ns, r.name_any()))
    },
).await?;
```

### Watch for changes

```rust
use kube_genops::watcher::{watch_resources, watch_resources_by_label};
use k8s_openapi::api::core::v1::ConfigMap;
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::channel(16);

// All ConfigMaps
let _handle = watch_resources::<ConfigMap>(client.clone(), tx.clone()).await?;

// Only those matching a label selector
let _handle = watch_resources_by_label::<ConfigMap>(client.clone(), tx, "app=my-operator").await?;

while let Some(()) = rx.recv().await {
    println!("ConfigMap changed — trigger reconcile");
}
```

### List resources

```rust
use kube_genops::resources::{list_resources, list_resources_by_label, list_namespaced_resources};
use k8s_openapi::api::core::v1::ConfigMap;

let all     = list_resources::<ConfigMap>(client.clone()).await?;
let labeled = list_resources_by_label::<ConfigMap>(client.clone(), "app=my-operator").await?;
let in_ns   = list_namespaced_resources::<ConfigMap>(client.clone(), "my-namespace").await?;
```

### Ensure a namespace exists

```rust
use kube_genops::resources::ensure_namespace;

ensure_namespace(client.clone(), "my-namespace", "my-operator").await?;
```

---

## Error handling

All functions return `Result<T, KubeGenericError>`:

```rust
pub enum KubeGenericError {
    Kube(kube::Error),
    MissingMetadata(String),
    Serialization(serde_json::Error),
    Other(anyhow::Error),
}
```

---

## Testing

Tests use `tower::service_fn` to mock the Kubernetes API — no cluster or kubeconfig needed:

```bash
cargo test
```

Enable log output:

```bash
RUST_LOG=kube_genops=debug cargo test -- --nocapture
```

To write your own tests:

```rust
use tower::service_fn;
use http::{Request, Response};
use kube::Client;

let svc = service_fn(|_req: Request<hyper::Body>| async move {
    Ok::<_, std::convert::Infallible>(
        Response::builder()
            .status(200)
            .header("content-type", "application/json")
            .body(hyper::Body::from(r#"{"metadata":{"name":"test"}}"#))
            .unwrap()
    )
});
let client = Client::new(svc, "default");
```

### Run the integration tests:

```bash
# Create a kind cluster
kind create cluster --name kube-genops-test

# Run
cargo test --features integration --test integration

# Tear down
kind delete cluster --name kube-genops-test
```


---

## Related

- [`kube-objstore`](../kube-objstore) — object store client for Kubernetes operators (Azure, S3, MinIO)

---

## License

MIT
