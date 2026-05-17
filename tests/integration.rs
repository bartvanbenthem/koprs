//! Integration tests for kube-genops.
//!
//! These tests run against a real Kubernetes cluster (local `kind` is recommended).
//!
//! # Prerequisites
//!
//! ```bash
//! # Install kind
//! brew install kind          # macOS
//! apt install kind           # Linux
//!
//! # Create a cluster
//! kind create cluster --name kube-genops-test
//!
//! # Run the integration tests
//! cargo test --features integration --test integration
//!
//! # Tear down afterwards
//! kind delete cluster --name kube-genops-test
//! ```
//!
//! Tests are fully isolated: each test creates resources with a unique suffix
//! and cleans up after itself, so they are safe to run concurrently.

#![cfg(feature = "integration")]

use std::collections::HashSet;

use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
use k8s_openapi::api::rbac::v1::ClusterRole;
use kube::Client;
use kube::api::ListParams;
use kube::core::ObjectMeta;
use kube::{Api, ResourceExt};
use kube_genops::finalizers::{
    add_cluster_finalizer, add_namespaced_finalizer, remove_cluster_finalizers,
    remove_namespaced_finalizers,
};
use kube_genops::gc::{gc_cluster_resources, gc_namespaced_resources};
use kube_genops::resources::{
    apply_cluster_resource, apply_namespaced_resource, delete_cluster_resource,
    delete_namespaced_resource, ensure_namespace, list_namespaced_resources,
    list_resources, list_resources_by_label,
};
use kube_genops::status::{patch_cluster_status, patch_namespaced_status};
use serde::{Deserialize, Serialize};

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

/// Unique suffix per test run to avoid name collisions across parallel tests.
fn uid(name: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("{}-{}", name, ts)
}

async fn client() -> Client {
    Client::try_default()
        .await
        .expect("Failed to build kube Client — is a cluster reachable?")
}

fn configmap(name: &str, namespace: &str, label: Option<&str>) -> ConfigMap {
    let mut labels = std::collections::BTreeMap::new();
    if let Some(l) = label {
        labels.insert("kube-genops-test".to_string(), l.to_string());
    }
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: if labels.is_empty() { None } else { Some(labels) },
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cluster_role(name: &str, label: Option<&str>) -> ClusterRole {
    let mut labels = std::collections::BTreeMap::new();
    if let Some(l) = label {
        labels.insert("kube-genops-test".to_string(), l.to_string());
    }
    ClusterRole {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: if labels.is_empty() { None } else { Some(labels) },
            ..Default::default()
        },
        ..Default::default()
    }
}

// =========================================================================
// ensure_namespace
// =========================================================================

#[tokio::test]
async fn test_ensure_namespace_creates_and_is_idempotent() {
    let client = client().await;
    let name = uid("genops-ns");

    // First call — creates
    ensure_namespace(client.clone(), &name, "kube-genops-test")
        .await
        .expect("ensure_namespace failed on first call");

    // Second call — idempotent
    ensure_namespace(client.clone(), &name, "kube-genops-test")
        .await
        .expect("ensure_namespace failed on second call");

    // Verify it exists
    let api: Api<Namespace> = Api::all(client.clone());
    api.get(&name).await.expect("Namespace not found after ensure");

    // Cleanup
    api.delete(&name, &Default::default()).await.ok();
}

// =========================================================================
// apply_namespaced_resource / delete_namespaced_resource
// =========================================================================

#[tokio::test]
async fn test_apply_and_delete_namespaced_configmap() {
    let client = client().await;
    let ns = "default";
    let name = uid("genops-cm");
    let cm = configmap(&name, ns, None);

    // Apply
    apply_namespaced_resource(client.clone(), ns, &cm, "kube-genops-test")
        .await
        .expect("apply_namespaced_resource failed");

    // Verify exists
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    api.get(&name).await.expect("ConfigMap not found after apply");

    // Delete
    let deleted = delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .expect("delete_namespaced_resource failed");
    assert!(deleted);

    // Delete again — should return false (404)
    let deleted_again = delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .expect("delete_namespaced_resource second call failed");
    assert!(!deleted_again);
}

#[tokio::test]
async fn test_apply_namespaced_is_idempotent() {
    let client = client().await;
    let ns = "default";
    let name = uid("genops-cm-idem");
    let cm = configmap(&name, ns, None);

    apply_namespaced_resource(client.clone(), ns, &cm, "kube-genops-test")
        .await
        .expect("first apply failed");

    apply_namespaced_resource(client.clone(), ns, &cm, "kube-genops-test")
        .await
        .expect("second apply failed — not idempotent");

    // Cleanup
    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}

// =========================================================================
// apply_cluster_resource / delete_cluster_resource
// =========================================================================

#[tokio::test]
async fn test_apply_and_delete_cluster_role() {
    let client = client().await;
    let name = uid("genops-cr");
    let cr = cluster_role(&name, None);

    // Apply
    apply_cluster_resource(client.clone(), &cr, "kube-genops-test")
        .await
        .expect("apply_cluster_resource failed");

    // Verify
    let api: Api<ClusterRole> = Api::all(client.clone());
    api.get(&name).await.expect("ClusterRole not found after apply");

    // Delete
    let deleted = delete_cluster_resource::<ClusterRole>(client.clone(), &name)
        .await
        .expect("delete_cluster_resource failed");
    assert!(deleted);

    // Delete again — 404
    let deleted_again = delete_cluster_resource::<ClusterRole>(client.clone(), &name)
        .await
        .expect("second delete failed unexpectedly");
    assert!(!deleted_again);
}

// =========================================================================
// list_resources / list_resources_by_label / list_namespaced_resources
// =========================================================================

#[tokio::test]
async fn test_list_namespaced_resources() {
    let client = client().await;
    let ns = "default";
    let name = uid("genops-list");
    let cm = configmap(&name, ns, None);

    apply_namespaced_resource(client.clone(), ns, &cm, "kube-genops-test")
        .await
        .unwrap();

    let list = list_namespaced_resources::<ConfigMap>(client.clone(), ns)
        .await
        .expect("list_namespaced_resources failed");

    assert!(
        list.items.iter().any(|c| c.name_any() == name),
        "Created ConfigMap not found in list"
    );

    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}

#[tokio::test]
async fn test_list_resources_by_label() {
    let client = client().await;
    let ns = "default";
    let label_value = uid("genops-label");
    let name = uid("genops-labeled");
    let cm = configmap(&name, ns, Some(&label_value));

    apply_namespaced_resource(client.clone(), ns, &cm, "kube-genops-test")
        .await
        .unwrap();

    let selector = format!("kube-genops-test={}", label_value);
    let list = list_resources_by_label::<ConfigMap>(client.clone(), &selector)
        .await
        .expect("list_resources_by_label failed");

    assert_eq!(list.items.len(), 1);
    assert_eq!(list.items[0].name_any(), name);

    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}

// =========================================================================
// Finalizers — namespaced
// =========================================================================

#[tokio::test]
async fn test_add_and_remove_namespaced_finalizer() {
    let client = client().await;
    let ns = "default";
    let name = uid("genops-fin");
    let cm = configmap(&name, ns, None);

    apply_namespaced_resource(client.clone(), ns, &cm, "kube-genops-test")
        .await
        .unwrap();

    // Add finalizer
    let with_fin =
        add_namespaced_finalizer::<ConfigMap>(client.clone(), ns, &name, "kube-genops/finalizer")
            .await
            .expect("add_namespaced_finalizer failed");

    assert!(
        with_fin
            .metadata
            .finalizers
            .as_deref()
            .unwrap_or_default()
            .contains(&"kube-genops/finalizer".to_string()),
        "Finalizer not present after add"
    );

    // Remove finalizers
    let without_fin = remove_namespaced_finalizers::<ConfigMap>(client.clone(), ns, &name)
        .await
        .expect("remove_namespaced_finalizers failed");

    assert!(
        without_fin
            .metadata
            .finalizers
            .as_deref()
            .unwrap_or_default()
            .is_empty(),
        "Finalizers still present after remove"
    );

    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}

// =========================================================================
// Finalizers — cluster-scoped
// =========================================================================

#[tokio::test]
async fn test_add_and_remove_cluster_finalizer() {
    let client = client().await;
    let name = uid("genops-cfin");
    let cr = cluster_role(&name, None);

    apply_cluster_resource(client.clone(), &cr, "kube-genops-test")
        .await
        .unwrap();

    let with_fin =
        add_cluster_finalizer::<ClusterRole>(client.clone(), &name, "kube-genops/finalizer")
            .await
            .expect("add_cluster_finalizer failed");

    assert!(
        with_fin
            .metadata
            .finalizers
            .as_deref()
            .unwrap_or_default()
            .contains(&"kube-genops/finalizer".to_string()),
        "Finalizer not present after add"
    );

    remove_cluster_finalizers::<ClusterRole>(client.clone(), &name)
        .await
        .expect("remove_cluster_finalizers failed");

    delete_cluster_resource::<ClusterRole>(client.clone(), &name)
        .await
        .ok();
}

// =========================================================================
// GC — cluster-scoped
// =========================================================================

#[tokio::test]
async fn test_gc_cluster_resources_deletes_orphans() {
    let client = client().await;
    let label = uid("gc-cluster");
    let selector = format!("kube-genops-test={}", label);

    let keep = uid("genops-gc-keep");
    let orphan = uid("genops-gc-orphan");

    // Create both
    apply_cluster_resource(client.clone(), &cluster_role(&keep, Some(&label)), "kube-genops-test")
        .await
        .unwrap();
    apply_cluster_resource(client.clone(), &cluster_role(&orphan, Some(&label)), "kube-genops-test")
        .await
        .unwrap();

    // GC — only keep in desired set
    let desired: HashSet<String> = [keep.clone()].into();
    gc_cluster_resources::<ClusterRole>(client.clone(), &selector, &desired)
        .await
        .expect("gc_cluster_resources failed");

    // Verify orphan is gone, keeper remains
    let api: Api<ClusterRole> = Api::all(client.clone());
    let remaining = api.list(&ListParams::default().labels(&selector)).await.unwrap();
    let names: Vec<_> = remaining.items.iter().map(|r| r.name_any()).collect();

    assert!(names.contains(&keep), "Kept resource was deleted");
    assert!(!names.contains(&orphan), "Orphaned resource was not deleted");

    // Cleanup
    delete_cluster_resource::<ClusterRole>(client.clone(), &keep).await.ok();
}

// =========================================================================
// GC — namespaced
// =========================================================================

#[tokio::test]
async fn test_gc_namespaced_resources_deletes_orphans() {
    let client = client().await;
    let ns = "default";
    let label = uid("gc-ns");
    let selector = format!("kube-genops-test={}", label);

    let keep = uid("genops-gc-ns-keep");
    let orphan = uid("genops-gc-ns-orphan");

    apply_namespaced_resource(
        client.clone(), ns,
        &configmap(&keep, ns, Some(&label)),
        "kube-genops-test",
    )
    .await
    .unwrap();
    apply_namespaced_resource(
        client.clone(), ns,
        &configmap(&orphan, ns, Some(&label)),
        "kube-genops-test",
    )
    .await
    .unwrap();

    let desired: HashSet<(String, String)> =
        [(ns.to_string(), keep.clone())].into();

    gc_namespaced_resources::<ConfigMap>(client.clone(), &selector, &desired)
        .await
        .expect("gc_namespaced_resources failed");

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    let remaining = api.list(&ListParams::default().labels(&selector)).await.unwrap();
    let names: Vec<_> = remaining.items.iter().map(|r| r.name_any()).collect();

    assert!(names.contains(&keep), "Kept ConfigMap was deleted");
    assert!(!names.contains(&orphan), "Orphaned ConfigMap was not deleted");

    // Cleanup
    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &keep).await.ok();
}