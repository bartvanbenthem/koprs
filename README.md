# Kubernetes Operators in Rust

[`koprs`](./crates/koprs) is a high-level Kubernetes operator library for Rust.

Operators simplify managing complex stateful applications on Kubernetes, but writing them remains difficult: low-level APIs, boilerplate, and poor modularity all add friction. Koprs is a Rust framework built on [`kube`](https://github.com/kube-rs/kube) and [`kube-runtime`](https://crates.io/crates/kube-runtime) that addresses this with high-level abstractions and extensions for common Operator use cases.

This repository contains the core framework and example operators.

## Crate

| Crate | Description | Docs |
|-------|-------------|------|
| [`koprs`](./crates/koprs) | Core generic runtime framework | [![docs.rs](https://img.shields.io/docsrs/koprs)](https://docs.rs/koprs) [![crates.io](https://img.shields.io/crates/v/koprs)](https://crates.io/crates/koprs) |

## Workspace layout

```
koprs/
├── Cargo.toml                  # workspace manifest
├── Cargo.lock
├── crates/
│   └── koprs/                  # core library
└── examples/
    ├── configmapsync/          # single CRD, single controller
    └── multicontroller/        # multiple CRDs, multiple controllers in one operator
```

## Getting started

If you are here to build a Kubernetes operator, you want [`koprs`](./crates/koprs). Start there.

For working end-to-end examples, see:

* [configmapsync](./examples/configmapsync/README.md) — a single CRD reconciled by one controller; the best starting point.
* [multicontroller](./examples/multicontroller/README.md) — multiple CRDs (`SecretSync`, `ServiceAccountSync`) each reconciled by its own controller, run side by side in one operator binary.

### Minimal example

A `koprs` operator boils down to three pieces: a CRD type, a [`Reconciler`](./crates/koprs/src/controller.rs),
and a [`ControllerBuilder`](./crates/koprs/src/controller.rs) that wires it all together.

```rust,no_run
use std::sync::Arc;
use std::time::Duration;

use kube::{Api, Client, CustomResource, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use koprs::controller::{Action, Context, ControllerBuilder, Reconciler};
use koprs::error::KubeGenericError;
use koprs::status::patch_status_namespaced;

/// The `Greeting` CRD — `kube::CustomResource` derives the type, its CRD spec,
/// and the generated `Greeting` struct (spec + status + metadata) in one go.
#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
#[kube(
    group = "example.io",
    version = "v1alpha1",
    kind = "Greeting",
    namespaced,
    status = "GreetingStatus"
)]
pub struct GreetingSpec {
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, JsonSchema)]
pub struct GreetingStatus {
    pub ready: bool,
}

struct GreetingReconciler;

impl Reconciler<Greeting> for GreetingReconciler {
    type Error = KubeGenericError;

    async fn reconcile(&self, cr: Arc<Greeting>, ctx: Arc<Context>) -> Result<Action, Self::Error> {
        let name = cr.name_any();
        let namespace = cr
            .namespace()
            .ok_or(KubeGenericError::MissingMetadata("namespace".into()))?;

        // Mark the resource ready — replace with your own reconciliation logic.
        patch_status_namespaced::<Greeting, GreetingStatus>(
            ctx.client.clone(),
            &namespace,
            &name,
            GreetingStatus { ready: true },
            "greeting-operator",
        )
        .await?;

        Ok(Action::requeue(Duration::from_secs(300)))
    }
    // error_policy defaults to requeue(30s) — override it for custom backoff
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let api: Api<Greeting> = Api::all(client.clone());
    let ctx = Context::new(client);

    ControllerBuilder::new(api)
        .health_port(8080)
        .graceful_shutdown()
        .run(GreetingReconciler, ctx)
        .await?;

    Ok(())
}
```

For finalizers, owned-resource reconciliation, garbage collection, events, and leader
election, see the [configmapsync operator](./examples/configmapsync/README.md), it
walks through the same building blocks in a complete, runnable operator. To see how to
run several CRDs and controllers from a single operator binary, see
[multicontroller](./examples/multicontroller/README.md).

## Contributing

Contributions are welcome. Please open an issue before submitting a pull request for 
anything beyond small fixes, so the approach can be agreed on first.

### Prerequisites

- Rust stable toolchain
- A local Kubernetes cluster for integration tests ([kind](https://kind.sigs.k8s.io/) recommended)

### Build

```bash
# build all crates
cargo build

# build a specific crate
cargo build -p koprs
```

### Test

```bash
# unit tests (no cluster required)
cargo test

# integration tests
kind create cluster --name koprs-test
cargo test --features integration --test integration
kind delete cluster --name koprs-test
```

### CI

[![CI](https://github.com/bartvanbenthem/koprs/actions/workflows/ci.yml/badge.svg)](https://github.com/bartvanbenthem/koprs/actions/workflows/ci.yml)

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

### Publishing

`publish.sh` handles the full pre-flight and publishes the crate to crates.io.

```bash
./scripts/publish.sh                    # full pre-flight + publish
./scripts/publish.sh --dry-run          # stop before cargo publish
./scripts/publish.sh --skip-ci          # skip CI checks, publish only
./scripts/publish.sh --crate koprs      # publish a single crate
```

See the [CI script docs](./scripts/cargo-ci.sh) for the full list of flags.

## License

MIT
