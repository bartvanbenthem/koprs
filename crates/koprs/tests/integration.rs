//! Integration tests for koprs.
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
//! kind create cluster --name koprs-test
//!
//! # Run the integration tests
//! cargo test --features integration --test integration
//!
//! # Tear down afterwards
//! kind delete cluster --name koprs-test
//! ```
//!
//! Tests are fully isolated: each test creates resources with a unique suffix
//! and cleans up after itself, so they are safe to run concurrently.

#![cfg(feature = "integration")]

use k8s_openapi::api::core::v1::{ConfigMap, Namespace};
use k8s_openapi::api::rbac::v1::ClusterRole;
use koprs::finalizers::{
    add_finalizer_cluster, add_finalizer_namespaced, remove_finalizers_cluster,
    remove_finalizers_namespaced,
};
use koprs::gc::{gc_cluster_resources, gc_namespaced_resources};
use koprs::resources::{
    apply_cluster_resource, apply_namespaced_resource, delete_cluster_resource,
    delete_namespaced_resource, ensure_namespace, list_namespaced_resources,
    list_resources_by_label,
};
use koprs::status::{patch_status_cluster, patch_status_namespaced};
use kube::api::ListParams;
use kube::core::ObjectMeta;
use kube::{Api, Client, ResourceExt};
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
        labels.insert("koprs-test".to_string(), l.to_string());
    }
    ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: if labels.is_empty() {
                None
            } else {
                Some(labels)
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

fn cluster_role(name: &str, label: Option<&str>) -> ClusterRole {
    let mut labels = std::collections::BTreeMap::new();
    if let Some(l) = label {
        labels.insert("koprs-test".to_string(), l.to_string());
    }
    ClusterRole {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            labels: if labels.is_empty() {
                None
            } else {
                Some(labels)
            },
            ..Default::default()
        },
        ..Default::default()
    }
}

// -------------------------------------------------------------------------
// Status types
// -------------------------------------------------------------------------

/// Minimal status struct used in patch_status tests. Any Serialize-able type
/// is accepted by patch_status_namespaced / patch_status_cluster.
#[derive(Serialize, Deserialize, Debug)]
struct TestStatus {
    ready: bool,
    message: String,
}

// =========================================================================
// ensure_namespace
// =========================================================================

#[tokio::test]
async fn test_ensure_namespace_creates_and_is_idempotent() {
    let client = client().await;
    let name = uid("genops-ns");

    // First call — creates
    ensure_namespace(client.clone(), &name, "koprs-test")
        .await
        .expect("ensure_namespace failed on first call");

    // Second call — idempotent (SSA, so no conflict)
    ensure_namespace(client.clone(), &name, "koprs-test")
        .await
        .expect("ensure_namespace failed on second call");

    // Verify it exists
    let api: Api<Namespace> = Api::all(client.clone());
    api.get(&name)
        .await
        .expect("Namespace not found after ensure");

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
    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
        .await
        .expect("apply_namespaced_resource failed");

    // Verify exists
    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    api.get(&name)
        .await
        .expect("ConfigMap not found after apply");

    // Delete — returns true when the resource existed
    let deleted = delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .expect("delete_namespaced_resource failed");
    assert!(deleted);

    // Delete again — returns false (404), must not error
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

    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
        .await
        .expect("first apply failed");

    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
        .await
        .expect("second apply failed — SSA must be idempotent");

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
    apply_cluster_resource(client.clone(), &cr, "koprs-test")
        .await
        .expect("apply_cluster_resource failed");

    // Verify
    let api: Api<ClusterRole> = Api::all(client.clone());
    api.get(&name)
        .await
        .expect("ClusterRole not found after apply");

    // Delete
    let deleted = delete_cluster_resource::<ClusterRole>(client.clone(), &name)
        .await
        .expect("delete_cluster_resource failed");
    assert!(deleted);

    // Delete again — 404, must not error
    let deleted_again = delete_cluster_resource::<ClusterRole>(client.clone(), &name)
        .await
        .expect("second delete failed unexpectedly");
    assert!(!deleted_again);
}

#[tokio::test]
async fn test_apply_cluster_resource_is_idempotent() {
    let client = client().await;
    let name = uid("genops-cr-idem");
    let cr = cluster_role(&name, None);

    apply_cluster_resource(client.clone(), &cr, "koprs-test")
        .await
        .expect("first apply_cluster_resource failed");

    apply_cluster_resource(client.clone(), &cr, "koprs-test")
        .await
        .expect("second apply_cluster_resource failed — SSA must be idempotent");

    // Cleanup
    delete_cluster_resource::<ClusterRole>(client.clone(), &name)
        .await
        .ok();
}

// =========================================================================
// list_resources_by_label / list_namespaced_resources
// =========================================================================

#[tokio::test]
async fn test_list_namespaced_resources() {
    let client = client().await;
    let ns = "default";
    let name = uid("genops-list");
    let cm = configmap(&name, ns, None);

    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
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

    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
        .await
        .unwrap();

    let selector = format!("koprs-test={}", label_value);
    let list = list_resources_by_label::<ConfigMap>(client.clone(), &selector)
        .await
        .expect("list_resources_by_label failed");

    assert_eq!(
        list.items.len(),
        1,
        "Expected exactly one ConfigMap with label {selector}"
    );
    assert_eq!(list.items[0].name_any(), name);

    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}

// =========================================================================
// patch_status_namespaced
// =========================================================================

#[tokio::test]
async fn test_patch_status_namespaced() {
    let client = client().await;
    let ns = "default";
    let name = uid("genops-status");
    let cm = configmap(&name, ns, None);

    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
        .await
        .unwrap();

    // ConfigMap has no /status subresource in most clusters, but the call must
    // reach the API server without a client-side error. A 404 on /status is
    // acceptable here — we are testing the request path, not the CRD schema.
    let result = patch_status_namespaced::<ConfigMap, _>(
        client.clone(),
        ns,
        &name,
        TestStatus {
            ready: true,
            message: "integration test".to_string(),
        },
        "koprs-test",
    )
    .await;

    // We accept Ok (CRD with status) or an API error (core resource without
    // /status). What must never happen is a client-side panic or serialisation
    // error, which would surface as a non-Api error variant.
    match result {
        Ok(_) => {}
        Err(koprs::error::KubeGenericError::Kube(kube::Error::Api(e))) => {
            // 404 = no /status subresource, 422 = schema validation — both
            // are server-side rejections, not client-side bugs.
            assert!(
                e.code == 404 || e.code == 405 || e.code == 422,
                "unexpected API error code {}: {}",
                e.code,
                e.message
            );
        }
        Err(e) => panic!("unexpected non-API error from patch_status_namespaced: {e:?}"),
    }

    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}

// =========================================================================
// patch_status_cluster
// =========================================================================

#[tokio::test]
async fn test_patch_status_cluster() {
    let client = client().await;
    let name = uid("genops-cr-status");
    let cr = cluster_role(&name, None);

    apply_cluster_resource(client.clone(), &cr, "koprs-test")
        .await
        .unwrap();

    let result = patch_status_cluster::<ClusterRole, _>(
        client.clone(),
        &name,
        TestStatus {
            ready: false,
            message: "cluster status test".to_string(),
        },
        "koprs-test",
    )
    .await;

    match result {
        Ok(_) => {}
        Err(koprs::error::KubeGenericError::Kube(kube::Error::Api(e))) => {
            assert!(
                e.code == 404 || e.code == 405 || e.code == 422,
                "unexpected API error code {}: {}",
                e.code,
                e.message
            );
        }
        Err(e) => panic!("unexpected non-API error from patch_status_cluster: {e:?}"),
    }

    delete_cluster_resource::<ClusterRole>(client.clone(), &name)
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

    apply_namespaced_resource(client.clone(), ns, &cm, "koprs-test")
        .await
        .unwrap();

    // Add finalizer
    let with_fin = add_finalizer_namespaced::<ConfigMap>(client.clone(), &cm, "koprs/finalizer")
        .await
        .expect("add_finalizer_namespaced failed");

    assert!(
        with_fin
            .metadata
            .finalizers
            .as_deref()
            .unwrap_or_default()
            .contains(&"koprs/finalizer".to_string()),
        "Finalizer not present after add"
    );

    // Remove all finalizers
    let without_fin = remove_finalizers_namespaced::<ConfigMap>(client.clone(), ns, &name)
        .await
        .expect("remove_finalizers_namespaced failed");

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

    apply_cluster_resource(client.clone(), &cr, "koprs-test")
        .await
        .unwrap();

    // Add finalizer
    let with_fin = add_finalizer_cluster::<ClusterRole>(client.clone(), &cr, "koprs/finalizer")
        .await
        .expect("add_finalizer_cluster failed");

    assert!(
        with_fin
            .metadata
            .finalizers
            .as_deref()
            .unwrap_or_default()
            .contains(&"koprs/finalizer".to_string()),
        "Finalizer not present after add"
    );

    // Remove all finalizers before deleting (otherwise the object gets stuck)
    remove_finalizers_cluster::<ClusterRole>(client.clone(), &name)
        .await
        .expect("remove_finalizers_cluster failed");

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
    let selector = format!("koprs-test={}", label);

    let keep = uid("genops-gc-keep");
    let orphan = uid("genops-gc-orphan");

    // Create both
    apply_cluster_resource(
        client.clone(),
        &cluster_role(&keep, Some(&label)),
        "koprs-test",
    )
    .await
    .unwrap();
    apply_cluster_resource(
        client.clone(),
        &cluster_role(&orphan, Some(&label)),
        "koprs-test",
    )
    .await
    .unwrap();

    // GC — predicate keeps only the "keep" resource by name.
    let keep_name = keep.clone();
    gc_cluster_resources::<ClusterRole>(client.clone(), &selector, move |r| {
        r.metadata.name.as_deref() == Some(&keep_name)
    })
    .await
    .expect("gc_cluster_resources failed");

    // Verify orphan is gone, keeper remains
    let api: Api<ClusterRole> = Api::all(client.clone());
    let remaining = api
        .list(&ListParams::default().labels(&selector))
        .await
        .unwrap();
    let names: Vec<_> = remaining.items.iter().map(|r| r.name_any()).collect();

    assert!(names.contains(&keep), "Kept resource was deleted");
    assert!(
        !names.contains(&orphan),
        "Orphaned resource was not deleted"
    );

    // Cleanup
    delete_cluster_resource::<ClusterRole>(client.clone(), &keep)
        .await
        .ok();
}

// =========================================================================
// GC — namespaced
// =========================================================================

#[tokio::test]
async fn test_gc_namespaced_resources_deletes_orphans() {
    let client = client().await;
    let ns = "default";
    let label = uid("gc-ns");
    let selector = format!("koprs-test={}", label);

    let keep = uid("genops-gc-ns-keep");
    let orphan = uid("genops-gc-ns-orphan");

    apply_namespaced_resource(
        client.clone(),
        ns,
        &configmap(&keep, ns, Some(&label)),
        "koprs-test",
    )
    .await
    .unwrap();
    apply_namespaced_resource(
        client.clone(),
        ns,
        &configmap(&orphan, ns, Some(&label)),
        "koprs-test",
    )
    .await
    .unwrap();

    // gc_namespaced_resources takes: client, namespace, label_selector, predicate.
    // The predicate receives a &ConfigMap and returns true for resources to keep.
    let keep_name = keep.clone();
    gc_namespaced_resources::<ConfigMap>(client.clone(), ns, &selector, move |r| {
        r.metadata.name.as_deref() == Some(&keep_name)
    })
    .await
    .expect("gc_namespaced_resources failed");

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    let remaining = api
        .list(&ListParams::default().labels(&selector))
        .await
        .unwrap();
    let names: Vec<_> = remaining.items.iter().map(|r| r.name_any()).collect();

    assert!(names.contains(&keep), "Kept ConfigMap was deleted");
    assert!(
        !names.contains(&orphan),
        "Orphaned ConfigMap was not deleted"
    );

    // Cleanup
    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &keep)
        .await
        .ok();
}

// =========================================================================
// GC — no-op when all resources are desired
// =========================================================================

#[tokio::test]
async fn test_gc_does_not_delete_desired_resources() {
    let client = client().await;
    let ns = "default";
    let label = uid("gc-noop");
    let selector = format!("koprs-test={}", label);

    let name = uid("genops-gc-keep-all");

    apply_namespaced_resource(
        client.clone(),
        ns,
        &configmap(&name, ns, Some(&label)),
        "koprs-test",
    )
    .await
    .unwrap();

    // Predicate always returns true — nothing should be deleted.
    gc_namespaced_resources::<ConfigMap>(client.clone(), ns, &selector, |_| true)
        .await
        .expect("gc_namespaced_resources failed");

    let api: Api<ConfigMap> = Api::namespaced(client.clone(), ns);
    api.get(&name)
        .await
        .expect("Resource was incorrectly deleted by GC");

    delete_namespaced_resource::<ConfigMap>(client.clone(), ns, &name)
        .await
        .ok();
}
