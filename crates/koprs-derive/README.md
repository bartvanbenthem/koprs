# KOPRS Derive 

Procedural macros for [`koprs`](https://crates.io/crates/koprs), the Kubernetes operator library for Rust.

This crate is an **implementation detail** of `koprs`. If you are building a Kubernetes operator, depend on `koprs` directly, it re-exports everything you need.

---

## What it provides

`koprs-derive` exposes the `#[derive(KoResource)]` macro. It generates the trait implementations required to make a custom CRD type work with `koprs`'s generic resource utilities:

- `kube::Resource` (with `DynamicType = ()`)
- `k8s_openapi::Metadata` (with `Ty = ObjectMeta`)
- The `koprs` marker traits `NamespacedResource` / `ClusterResource`

Without the macro you must implement these by hand. With it:

```rust
use koprs_derive::KoResource;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, KoResource)]
pub struct MyOperatorCrd {
    pub metadata: ObjectMeta,
    pub spec: MySpec,
    pub status: Option<MyStatus>,
}
```

---

## License

MIT
