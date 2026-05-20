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
kube-genops = { path = "../kube-genops" }
# or once published:
# kube-genops = "0.2.1"
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
use kube_genops::resources::{
    apply_resource, apply_cluster_resource, apply_namespaced_resource,
};
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::resources::{
    delete_resource, delete_cluster_resource, delete_namespaced_resource,
};
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::resources::ensure_namespace;

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
use kube_genops::status::{patch_status, patch_status_cluster, patch_status_namespaced};
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
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::finalizers::{
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
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::gc::{gc_cluster_resources, gc_namespaced_resources, gc_resources};
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::watcher::{
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
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::resources::{
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
use kube_genops::resources::{
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
use kube_genops::scope::{Cluster, Namespaced};

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
use kube_genops::resources::{
    make_object_refs, make_object_refs_cluster, make_object_refs_namespaced,
    make_object_ref_mapper,
};
use kube_genops::scope::{Cluster, Namespaced};
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
use kube_genops::resources::fetch_and_write_to_file;
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
use kube_genops::error::KubeGenericError;

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

- [`kube-objstore`](../kube-objstore) — object store client for Kubernetes operators

---

## License

MIT
