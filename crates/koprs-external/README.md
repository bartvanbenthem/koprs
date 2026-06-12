# koprs-external

Generic polling watchers for external sources such as HTTP REST APIs and object stores, designed as a companion to [`koprs`](../koprs) Kubernetes operators.

Kubernetes operators often need to reconcile cluster state with resources that live outside the cluster — a remote configuration endpoint, an object store, or a third-party API. `koprs-external` provides a lightweight polling abstraction that fits naturally alongside `koprs` controllers, using the same channel-based pattern as `koprs::watcher`.


## Architecture Overview

`koprs-external` sits alongside your operator and bridges external HTTP or object-store sources into the same `mpsc` channel model used by `koprs::watcher`.

```bash
+-------------------------------------------------------+
|                 Your Operator App                     |
|  (Reconcile Kubernetes state from external sources)   |
+-------------------------------------------------------+
          |                            |
          v                            v
+------------------+       +---------------------------+
|    koprs         |       |    koprs-external         |
|  (Kubernetes     |       |  (HTTP polling,           |
|   API watcher)   |       |   object-store diffing)   |
+------------------+       +---------------------------+
          |                            |
          v                            v
+------------------+       +---------------------------+
|    kube-rs       |       |  reqwest / object_store   |
+------------------+       |  (S3, GCS, Azure, local)  |
                            +---------------------------+
```

---

## What koprs-external provides

| Area | koprs-external | rolling your own |
|---|---|---|
| **Polling loop** | `watch_external` spawns a background task that ticks on a configurable interval | You write the loop, interval logic, and missed-tick handling |
| **Change detection** | ETag / `304 Not Modified` support; falls back to `Last-Modified`; tracks added, modified, and removed items | You diff results yourself on every poll |
| **Event model** | `ExternalEvent<T>` with `Added`, `Modified`, `Removed` variants — same shape as `koprs::WatchEvent` | You define and maintain your own event type |
| **Authentication** | Bearer token and arbitrary request headers via fluent builder | You build headers on every request |
| **Object store support** | ETag-based diffing over any `object_store`-compatible backend — S3, GCS, Azure, local, HTTP, in-memory (feature-gated) | You call the list API and diff the results yourself |
| **Error handling** | Poll errors are logged and retried automatically; the watcher never stops on a transient failure | You decide whether to panic, log, or back off |
| **Tracing** | All poll activity and errors are emitted as structured `tracing` spans | You wire up logging yourself |

---

## Features

| Cargo feature | What it enables |
|---|---|
| _(default)_ | `HttpPoller` — polls any HTTP or HTTPS endpoint |
| `object-store` | `ObjectStorePoller` — lists and diffs any [`object_store`](https://docs.rs/object_store)-compatible backend (S3, GCS, Azure Blob, local filesystem, in-memory) |
| `integration` | Enables `tests/integration.rs` (requires `--features integration` to compile) |

> **Object store backends** are opt-in via `object_store`'s own feature flags. Add the backend you need in your application's `Cargo.toml` (e.g. `object_store = { version = "0.11", features = ["aws"] }` for S3).

---

## Module overview

| Module | Description |
|---|---|
| `watcher` | `ExternalSource` trait, `ExternalEvent<T>` enum, `watch_external` spawner |
| `http` | `HttpPoller` — polls a single HTTP endpoint; ETag / `Last-Modified` change detection |
| `store` | `ObjectStorePoller` — lists and diffs any `object_store` backend (requires `object-store` feature) |
| `error` | `ExternalError` enum |

---

## Installation

```toml
[dependencies]
koprs-external = { path = "../koprs-external" }
# or once published:
# koprs-external = "<version>"

# Optional S3 support
# koprs-external = { version = "<version>", features = ["s3"] }
```

---

## Usage

### HTTP API — polling a REST endpoint

`HttpPoller` polls a single URL. The first successful response emits `Added`, subsequent changes emit `Modified`, and a `404` after a prior success emits `Removed`. ETag-based conditional requests (`304 Not Modified`) are handled automatically.

```rust
use std::time::Duration;
use koprs_external::http::HttpPoller;
use koprs_external::watcher::{watch_external, ExternalEvent};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(16);

    let poller = HttpPoller::new("https://api.example.com/config")
        .with_bearer_token("my-token")
        .with_name("config-api");

    let _handle = watch_external(poller, Duration::from_secs(30), tx);

    while let Some(event) = rx.recv().await {
        match event {
            ExternalEvent::Added(r)    => println!("appeared: {} bytes", r.body.len()),
            ExternalEvent::Modified(r) => println!("changed:  {} bytes", r.body.len()),
            ExternalEvent::Removed(_)  => println!("gone"),
        }
    }
}
```

### HTTP API — custom TLS (e.g. Kubernetes API server)

Bring your own `reqwest::Client` for mutual TLS, custom CA certificates, or
connection timeouts. This pattern works against the Kubernetes REST API when
combined with a bearer token:

```rust
use koprs_external::http::HttpPoller;

let client = reqwest::Client::builder()
    .add_root_certificate(/* your cluster CA cert */)
    .build()
    .unwrap();

let poller = HttpPoller::new("https://kubernetes.default.svc/api/v1/namespaces/default/configmaps/my-config")
    .with_client(client)
    .with_bearer_token(&token);
```

### HTTP API — implementing a custom source

For sources that do not fit the single-URL model, implement `ExternalSource`
directly:

```rust
use futures::future::BoxFuture;
use koprs_external::error::Result;
use koprs_external::watcher::{ExternalEvent, ExternalSource};

struct MyApiPoller { /* ... */ }

impl ExternalSource for MyApiPoller {
    type Item = MyData;

    fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<MyData>>>> {
        Box::pin(async move {
            // fetch, diff against self.last_state, return events
            todo!()
        })
    }

    fn name(&self) -> &str { "my-api" }
}
```

### Object store — polling for object changes

`ObjectStorePoller` accepts any `Arc<dyn ObjectStore>`. The backend is
configured entirely outside the poller — swap S3 for GCS or a local
directory without changing any polling code.

**AWS S3** (add `object_store = { version = "0.11", features = ["aws"] }` to your `Cargo.toml`):

```rust
#[cfg(feature = "object-store")]
use std::sync::Arc;
#[cfg(feature = "object-store")]
use object_store::aws::AmazonS3Builder;
#[cfg(feature = "object-store")]
use koprs_external::store::ObjectStorePoller;
use koprs_external::watcher::{watch_external, ExternalEvent};
use std::time::Duration;
use tokio::sync::mpsc;

#[cfg(feature = "object-store")]
#[tokio::main]
async fn main() {
    let store = Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name("my-bucket")
            .build()
            .unwrap(),
    );

    let (tx, mut rx) = mpsc::channel(16);
    let poller = ObjectStorePoller::new(store).with_prefix("configs/");
    let _handle = watch_external(poller, Duration::from_secs(60), tx);

    while let Some(event) = rx.recv().await {
        match event {
            ExternalEvent::Added(obj)    => println!("new:     {} ({} B)", obj.path, obj.size),
            ExternalEvent::Modified(obj) => println!("changed: {}", obj.path),
            ExternalEvent::Removed(obj)  => println!("deleted: {}", obj.path),
        }
    }
}
```

The same poller works unchanged against **GCS**, **Azure Blob**, or a **local
directory** — just swap the builder:

```rust
// Google Cloud Storage
use object_store::gcp::GoogleCloudStorageBuilder;
let store = Arc::new(GoogleCloudStorageBuilder::from_env().with_bucket_name("my-bucket").build().unwrap());

// Local filesystem
use object_store::local::LocalFileSystem;
let store = Arc::new(LocalFileSystem::new_with_prefix("/data/configs").unwrap());
```
```

---

## Testing

### Unit tests

Unit tests use an in-process axum HTTP server — no external services required.

```bash
cargo test -p koprs-external
```

### Integration tests

The HTTP integration tests spin up a local server and exercise the full polling
loop end-to-end. They do not require a Kubernetes cluster or AWS credentials.

```bash
cargo test -p koprs-external --features integration --test integration
```

### Kubernetes integration test

One test (`kubernetes_configmap_lifecycle_via_http_poller`) is marked
`#[ignore]` and requires a reachable cluster with a service-account token. See
the header of `tests/integration.rs` for step-by-step setup instructions using
`kind`.

```bash
# After completing the setup in tests/integration.rs:
cargo test -p koprs-external --features integration --test integration \
    -- --include-ignored
```

### Object store tests

`ObjectStorePoller` unit tests use the built-in `InMemory` backend from
`object_store` — no AWS credentials, no LocalStack, no external services
required. All event types (Added, Modified, Removed) and prefix filtering
are covered by these tests.

```bash
cargo test -p koprs-external --features object-store
```
