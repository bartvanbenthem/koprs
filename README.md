# Kubernetes Generic Operations

The monorepo behind `koprs`, a high-level Kubernetes operator framework for Rust.

`koprs` is the idiomatic way to build Kubernetes operators in Rust. It is built on top of 
[`kube`](https://github.com/kube-rs/kube) and `kube-runtime`, providing the opinionated 
structure and reusable abstractions that production operators need — without forcing you to 
reinvent them for every project.

This repository contains the core framework, its proc macros, and the manifest generation 
tooling for CRDs and RBAC.

## Crates

| Crate | Description | Docs |
|-------|-------------|------|
| [`koprs`](./crates/koprs) | Core generic runtime framework | [![docs.rs](https://img.shields.io/docsrs/koprs)](https://docs.rs/koprs) [![crates.io](https://img.shields.io/crates/v/koprs)](https://crates.io/crates/koprs) |
| [`koprs-derive`](./crates/koprs-derive) | Proc macros — implementation detail | [![docs.rs](https://img.shields.io/docsrs/koprs-derive)](https://docs.rs/koprs-derive) [![crates.io](https://img.shields.io/crates/v/koprs-derive)](https://crates.io/crates/koprs-derive) |
| [`koprs-gen`](./crates/koprs-gen) | CRD and RBAC manifest generation CLI | [![docs.rs](https://img.shields.io/docsrs/koprs-gen)](https://docs.rs/koprs-gen) [![crates.io](https://img.shields.io/crates/v/koprs-gen)](https://crates.io/crates/koprs-gen) |

## Workspace layout

```
kube-genops/
├── Cargo.toml                  # workspace manifest
├── Cargo.lock
└── crates/
    ├── koprs/                  # core library
    ├── koprs-derive/           # proc macros
    └── koprs-gen/              # codegen CLI
```

## Getting started

If you are here to build a Kubernetes operator, you want [`koprs`](./crates/koprs). Start there.

If you want to generate CRD or RBAC manifests from your annotated Rust types, you want [`koprs-gen`](./crates/koprs-gen).

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
kind create cluster --name kube-genops-test
cargo test --features integration --test integration
kind delete cluster --name kube-genops-test
```

### CI

```bash
./scripts/cargo-ci.sh           # run all checks
./scripts/cargo-ci.sh --fast    # fmt + check + unit tests only
```

See the [CI script docs](./scripts/cargo-ci.sh) for the full list of flags.

## License

MIT
