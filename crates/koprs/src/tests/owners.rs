// src/tests/owners.rs
//
// Unit tests for the `owners` module.
//
// Strategy
// --------
// Owner reference helpers are pure functions over in-memory structs — no HTTP
// call is ever made. Tests construct `ConfigMap` and `Deployment` values
// directly from JSON, call the helpers, and assert on the returned fields.

#[cfg(test)]
mod owners_tests {
    use k8s_openapi::api::apps::v1::Deployment;
    use k8s_openapi::api::core::v1::ConfigMap;
    use kube::Resource;
    use serde_json::json;

    use crate::owners::{controller_ref, owner_ref, set_owner_refs};

    // -----------------------------------------------------------------------
    // Test helpers
    // -----------------------------------------------------------------------

    fn configmap_with_uid(name: &str, namespace: &str, uid: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "namespace": namespace, "uid": uid, "resourceVersion": "1" }
        }))
        .unwrap()
    }

    fn configmap_no_uid(name: &str) -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "name": name, "resourceVersion": "1" }
        }))
        .unwrap()
    }

    fn configmap_no_name() -> ConfigMap {
        serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": { "uid": "abc-123", "resourceVersion": "1" }
        }))
        .unwrap()
    }

    fn empty_deployment() -> Deployment {
        serde_json::from_value(json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "child", "namespace": "default", "resourceVersion": "1" }
        }))
        .unwrap()
    }

    // -----------------------------------------------------------------------
    // owner_ref
    // -----------------------------------------------------------------------

    #[test]
    fn owner_ref_builds_correct_fields() {
        let cm = configmap_with_uid("my-cm", "default", "uid-abc");
        let oref = owner_ref(&cm).unwrap();

        assert_eq!(oref.name, "my-cm");
        assert_eq!(oref.uid, "uid-abc");
        assert_eq!(oref.kind, "ConfigMap");
        assert_eq!(oref.api_version, ConfigMap::api_version(&()).as_ref());
    }

    #[test]
    fn owner_ref_sets_controller_false() {
        let cm = configmap_with_uid("my-cm", "default", "uid-abc");
        let oref = owner_ref(&cm).unwrap();

        assert_eq!(oref.controller, Some(false));
        assert_eq!(oref.block_owner_deletion, Some(false));
    }

    #[test]
    fn owner_ref_errors_on_missing_uid() {
        let cm = configmap_no_uid("my-cm");
        let result = owner_ref(&cm);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("uid"), "expected 'uid' in error: {msg}");
    }

    #[test]
    fn owner_ref_errors_on_missing_name() {
        let cm = configmap_no_name();
        let result = owner_ref(&cm);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("name"), "expected 'name' in error: {msg}");
    }

    // -----------------------------------------------------------------------
    // controller_ref
    // -----------------------------------------------------------------------

    #[test]
    fn controller_ref_sets_controller_true() {
        let cm = configmap_with_uid("my-cm", "default", "uid-xyz");
        let oref = controller_ref(&cm).unwrap();

        assert_eq!(oref.controller, Some(true));
        assert_eq!(oref.block_owner_deletion, Some(true));
    }

    #[test]
    fn controller_ref_builds_correct_fields() {
        let cm = configmap_with_uid("my-cm", "default", "uid-xyz");
        let oref = controller_ref(&cm).unwrap();

        assert_eq!(oref.name, "my-cm");
        assert_eq!(oref.uid, "uid-xyz");
        assert_eq!(oref.kind, "ConfigMap");
    }

    // -----------------------------------------------------------------------
    // set_owner_refs
    // -----------------------------------------------------------------------

    #[test]
    fn set_owner_refs_writes_owner_references_to_child() {
        let cm = configmap_with_uid("parent", "default", "uid-parent");
        let oref = controller_ref(&cm).unwrap();

        let mut child = empty_deployment();
        set_owner_refs(&mut child, vec![oref.clone()]);

        let refs = child.meta().owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "parent");
        assert_eq!(refs[0].uid, "uid-parent");
        assert_eq!(refs[0].controller, Some(true));
    }

    #[test]
    fn set_owner_refs_replaces_existing_references() {
        let cm1 = configmap_with_uid("parent-1", "default", "uid-1");
        let cm2 = configmap_with_uid("parent-2", "default", "uid-2");

        let mut child = empty_deployment();
        set_owner_refs(&mut child, vec![owner_ref(&cm1).unwrap()]);
        set_owner_refs(&mut child, vec![controller_ref(&cm2).unwrap()]);

        let refs = child.meta().owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "parent-2");
    }

    #[test]
    fn set_owner_refs_accepts_multiple_owners() {
        let cm1 = configmap_with_uid("p1", "default", "uid-1");
        let cm2 = configmap_with_uid("p2", "default", "uid-2");

        let mut child = empty_deployment();
        set_owner_refs(
            &mut child,
            vec![owner_ref(&cm1).unwrap(), owner_ref(&cm2).unwrap()],
        );

        let refs = child.meta().owner_references.as_ref().unwrap();
        assert_eq!(refs.len(), 2);
    }
}
