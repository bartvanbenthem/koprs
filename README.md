# kube-genops

A lightweight, opinionated wrapper library for `kube-rs` that abstracts away repetitive boilerplate, lifecycle patterns, and generic CRUD/GC operations for Kubernetes custom controllers.

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

## Features

* **Generic Lifecycle Janitors:** Block initialization until required singleton Custom Resources appear, or trigger automatic operator shutdowns when a cluster is emptied of target resources.
* **Streamlined Finalizers:** Single-line addition and removal of Kubernetes finalizers without rewriting JSON merge patch logic.
* **Unified Status Patching:** Simple, type-safe status updates for both namespaced and cluster-scoped resources.
* **Automated Garbage Collection:** Quick cleanup mechanisms for removing child resources using label selectors.

## Installation

Add `kube-genops` to your `Cargo.toml`:

```toml
[dependencies]
kube-genops = { path = "../kube-genops" } # Or via git/registry
```