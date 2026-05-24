# Kubernetes Generic Operations

Monorepo for the `koprs` Kubernetes operator framework for Rust.

## Crates

| Crate | Description | Docs |
|-------|-------------|------|
| [`koprs`](./crates/koprs) | Core runtime framework — SSA, finalizers, GC, status patching, watchers | [![docs.rs](https://img.shields.io/docsrs/koprs)](https://docs.rs/koprs) [![crates.io](https://img.shields.io/crates/v/koprs)](https://crates.io/crates/koprs) |
| [`koprs-derive`](./crates/koprs-derive) | Proc macros — implementation detail, re-exported by `koprs` | [![docs.rs](https://img.shields.io/docsrs/koprs-derive)](https://docs.rs/koprs-derive) [![crates.io](https://img.shields.io/crates/v/koprs-derive)](https://crates.io/crates/koprs-derive) |
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
