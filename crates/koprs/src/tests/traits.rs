// src/tests/traits.rs
//
// Testing strategy
// ----------------
// KubeResource, NamespacedResource, and ClusterResource are pure compile-time
// constraints with blanket impls and no runtime behaviour. There is nothing to
// mock and no value to assert on at runtime.
//
// The correct test style is *instantiation tests*: prove that concrete types
// satisfy (or do not satisfy) the expected bounds by asking the compiler to
// resolve them. If a bound is missing or incorrectly wired, the test file
// fails to compile — which is itself the test result.
//
// Negative tests (things that must NOT compile) are expressed as
// `compile_fail` doc-tests in the source rather than here, because
// `#[cfg(test)]` cannot gate "this must not compile" within a test binary.
// See the "Compile-time rejection" section below for the corresponding
// doc-test anchors.

#[cfg(test)]
mod traits_tests {
    use k8s_openapi::api::core::v1::{ConfigMap, Namespace, Node, Pod, Secret, ServiceAccount};
    use k8s_openapi::api::rbac::v1::{ClusterRole, ClusterRoleBinding};

    use crate::traits::{ClusterResource, KubeResource, NamespacedResource};

    // -----------------------------------------------------------------------
    // Helper — zero-cost bound witness
    // -----------------------------------------------------------------------
    //
    // Calling `assert_kube_resource::<T>()` is a compile-time proof that T
    // implements KubeResource. The function is generic, never called at runtime
    // in a meaningful way, and produces no object code.

    fn assert_kube_resource<T: KubeResource>() {}
    fn assert_namespaced<T: NamespacedResource>() {}
    fn assert_cluster<T: ClusterResource>() {}

    // -----------------------------------------------------------------------
    // KubeResource — satisfied by both namespaced and cluster-scoped types
    // -----------------------------------------------------------------------

    #[test]
    fn pod_satisfies_kube_resource() {
        assert_kube_resource::<Pod>();
    }

    #[test]
    fn configmap_satisfies_kube_resource() {
        assert_kube_resource::<ConfigMap>();
    }

    #[test]
    fn secret_satisfies_kube_resource() {
        assert_kube_resource::<Secret>();
    }

    #[test]
    fn service_account_satisfies_kube_resource() {
        assert_kube_resource::<ServiceAccount>();
    }

    #[test]
    fn node_satisfies_kube_resource() {
        assert_kube_resource::<Node>();
    }

    #[test]
    fn cluster_role_satisfies_kube_resource() {
        assert_kube_resource::<ClusterRole>();
    }

    #[test]
    fn namespace_satisfies_kube_resource() {
        // Namespace is itself a cluster-scoped resource — verifies the trait
        // does not accidentally restrict to namespaced-only types.
        assert_kube_resource::<Namespace>();
    }

    // -----------------------------------------------------------------------
    // NamespacedResource — only namespace-scoped k8s types
    // -----------------------------------------------------------------------

    #[test]
    fn pod_satisfies_namespaced_resource() {
        assert_namespaced::<Pod>();
    }

    #[test]
    fn configmap_satisfies_namespaced_resource() {
        assert_namespaced::<ConfigMap>();
    }

    #[test]
    fn secret_satisfies_namespaced_resource() {
        assert_namespaced::<Secret>();
    }

    #[test]
    fn service_account_satisfies_namespaced_resource() {
        assert_namespaced::<ServiceAccount>();
    }

    // -----------------------------------------------------------------------
    // ClusterResource — only cluster-scoped k8s types
    // -----------------------------------------------------------------------

    #[test]
    fn node_satisfies_cluster_resource() {
        assert_cluster::<Node>();
    }

    #[test]
    fn cluster_role_satisfies_cluster_resource() {
        assert_cluster::<ClusterRole>();
    }

    #[test]
    fn cluster_role_binding_satisfies_cluster_resource() {
        assert_cluster::<ClusterRoleBinding>();
    }

    #[test]
    fn namespace_satisfies_cluster_resource() {
        // Namespace is itself cluster-scoped in Kubernetes.
        assert_cluster::<Namespace>();
    }

    // -----------------------------------------------------------------------
    // Scope exclusivity — a type cannot satisfy both scope markers
    // -----------------------------------------------------------------------
    //
    // Pod is namespaced, Node is cluster-scoped. Each satisfies exactly one
    // scope marker trait. These tests confirm the scopes don't overlap at the
    // type level by showing the correct one compiles; the corresponding
    // negative case is covered by compile_fail doc-tests on the trait
    // definitions themselves.

    #[test]
    fn pod_is_namespaced_not_cluster() {
        // This compiles → Pod: NamespacedResource
        assert_namespaced::<Pod>();
        // The inverse (`assert_cluster::<Pod>()`) must not compile.
        // Covered by the compile_fail doc-test on ClusterResource.
    }

    #[test]
    fn node_is_cluster_not_namespaced() {
        // This compiles → Node: ClusterResource
        assert_cluster::<Node>();
        // The inverse (`assert_namespaced::<Node>()`) must not compile.
        // Covered by the compile_fail doc-test on NamespacedResource.
    }

    // -----------------------------------------------------------------------
    // Supertrait relationship — NamespacedResource and ClusterResource
    // both imply KubeResource
    // -----------------------------------------------------------------------

    #[test]
    fn namespaced_resource_implies_kube_resource() {
        // If NamespacedResource: KubeResource holds, then any function
        // accepting KubeResource will also accept a NamespacedResource.
        // We verify this by passing a namespaced type to assert_kube_resource.
        assert_kube_resource::<Pod>();
        assert_namespaced::<Pod>();
    }

    #[test]
    fn cluster_resource_implies_kube_resource() {
        assert_kube_resource::<Node>();
        assert_cluster::<Node>();
    }

    // -----------------------------------------------------------------------
    // Trait object safety is NOT required — these are generic bounds only.
    // The traits intentionally include non-object-safe supertraits (Clone,
    // Sized-implied Resource), so no dyn-cast tests are included.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Auto-impl — user-defined types that satisfy the bounds are accepted
    // -----------------------------------------------------------------------
    //
    // Verifies that the blanket impl picks up types outside of k8s-openapi,
    // e.g. CRDs defined in the operator crate itself. We construct a minimal
    // synthetic type that satisfies every required bound.

    mod synthetic {
        use kube::core::ObjectMeta;
        use serde::{Deserialize, Serialize};
        use std::borrow::Cow;

        // A minimal CRD-like type that satisfies all KubeResource bounds.
        // Mirrors what kube::CustomResource derive generates: a direct kube::Resource
        // impl without going through k8s_openapi::Metadata. This is the pattern that
        // previously failed to satisfy KubeResource.
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub struct FakeCrd {
            pub metadata: ObjectMeta,
        }

        impl kube::Resource for FakeCrd {
            type DynamicType = ();
            type Scope = k8s_openapi::NamespaceResourceScope;

            fn group(_: &()) -> Cow<'_, str> {
                Cow::Borrowed("example.com")
            }
            fn version(_: &()) -> Cow<'_, str> {
                Cow::Borrowed("v1")
            }
            fn kind(_: &()) -> Cow<'_, str> {
                Cow::Borrowed("FakeCrd")
            }
            fn plural(_: &()) -> Cow<'_, str> {
                Cow::Borrowed("fakecrds")
            }
            fn meta(&self) -> &ObjectMeta {
                &self.metadata
            }
            fn meta_mut(&mut self) -> &mut ObjectMeta {
                &mut self.metadata
            }
        }
    }

    #[test]
    fn user_defined_namespaced_crd_satisfies_kube_resource() {
        assert_kube_resource::<synthetic::FakeCrd>();
    }

    #[test]
    fn user_defined_namespaced_crd_satisfies_namespaced_resource() {
        assert_namespaced::<synthetic::FakeCrd>();
    }
}
