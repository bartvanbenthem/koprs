# kube-genops

A reusable library of generic, ergonomic functions for the most common Kubernetes operator patterns. Built on top of [`kube`](https://docs.rs/kube) and [`kube-runtime`](https://docs.rs/kube-runtime), it eliminates boilerplate across operators by providing a single, well-tested implementation for each pattern.


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

```rust
use kube_genops::status::{patch_cluster_status, patch_namespaced_status};
use serde::Serialize;

#[derive(Serialize)]
struct MyStatus { ready: bool, message: String }

// Cluster-scoped CRD
patch_cluster_status::<MyCR, _>(
    client.clone(), "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;

// Namespaced CRD
patch_namespaced_status::<MyCR, _>(
    client.clone(), "my-namespace", "my-resource",
    MyStatus { ready: true, message: "Reconciled".into() },
    "my-operator",
).await?;
```

### Manage finalizers

```rust
use kube_genops::finalizers::{
    add_namespaced_finalizer, remove_namespaced_finalizers,
    add_cluster_finalizer, remove_cluster_finalizers,
};
use k8s_openapi::api::core::v1::{ConfigMap, PersistentVolume};

// Add
add_namespaced_finalizer::<ConfigMap>(client.clone(), "my-ns", "my-cm", "my-operator/finalizer").await?;
add_cluster_finalizer::<PersistentVolume>(client.clone(), "my-pv", "my-operator/finalizer").await?;

// Remove all
remove_namespaced_finalizers::<ConfigMap>(client.clone(), "my-ns", "my-cm").await?;
remove_cluster_finalizers::<PersistentVolume>(client.clone(), "my-pv").await?;
```

### Garbage collect orphaned resources

```rust
use std::collections::HashSet;
use kube_genops::gc::{gc_cluster_resources, gc_namespaced_resources};
use k8s_openapi::api::core::v1::{PersistentVolume, PersistentVolumeClaim};

// Cluster-scoped — delete any PV labelled "app=my-operator" not in desired_names
let desired = HashSet::from(["pv-a".to_string(), "pv-b".to_string()]);
gc_cluster_resources::<PersistentVolume>(client.clone(), "app=my-operator", &desired).await?;

// Namespaced — keyed on (namespace, name) pairs
let desired = HashSet::from([("default".to_string(), "pvc-a".to_string())]);
gc_namespaced_resources::<PersistentVolumeClaim>(client.clone(), "app=my-operator", &desired).await?;
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
