# Kubernetes Operator SDK for Rust

[`koprs`](./crates/koprs) is a high-level Kubernetes operator library for Rust.

Operators make it easy to manage complex stateful applications on top of Kubernetes. However writing an Operator today can be difficult because of challenges such as using low level APIs, writing boilerplate, and a lack of modularity which leads to duplication.

The Operator SDK for Rust is a framework that uses [`kube`](https://github.com/kube-rs/kube) and [`kube-runtime`](https://crates.io/crates/kube-runtime) libraries to make writing operators easier by providing:

* High level APIs and abstractions to write the operational logic more intuitively
* Tools for scaffolding and code generation to bootstrap a new project fast
* Extensions to cover common Operator use cases

This repository contains the core framework.

## Crates

| Crate | Description | Docs |
|-------|-------------|------|
| [`koprs`](./crates/koprs) | Core generic runtime framework | [![docs.rs](https://img.shields.io/docsrs/koprs)](https://docs.rs/koprs) [![crates.io](https://img.shields.io/crates/v/koprs)](https://crates.io/crates/koprs) |

## Workspace layout

```
koprs/
├── Cargo.toml                  # workspace manifest
├── Cargo.lock
└── crates/
    └── koprs/                  # core library
```

## Getting started

If you are here to build a Kubernetes operator, you want [`koprs`](./crates/koprs). Start there.

For a working end-to-end example, see the [configmapsync operator](./examples/configmapsync/README.md).

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
