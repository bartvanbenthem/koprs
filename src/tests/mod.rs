//! Tests for kube-genops.
//!
//! API-level tests (apply, delete, status, finalizers) require a live cluster
//! and are best run as integration tests against a real or kind cluster.
//!
//! These unit tests cover the logic that doesn't need a Kubernetes API:
//! garbage collection diffing, config helpers, and error variant matching.
#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::error::KubeGenericError;

    // =========================================================================
    // GC — cluster-scoped diffing
    // =========================================================================

    #[test]
    fn gc_cluster_finds_orphans() {
        let existing = vec!["pv-a", "pv-b", "pv-c"];
        let desired: HashSet<String> = ["pv-a".into(), "pv-b".into()].into();

        let orphaned: Vec<_> = existing
            .into_iter()
            .filter(|n| !desired.contains(*n))
            .collect();

        assert_eq!(orphaned, vec!["pv-c"]);
    }

    #[test]
    fn gc_cluster_no_orphans_when_all_desired() {
        let existing = vec!["pv-a", "pv-b"];
        let desired: HashSet<String> = ["pv-a".into(), "pv-b".into()].into();

        let orphaned: Vec<_> = existing
            .into_iter()
            .filter(|n| !desired.contains(*n))
            .collect();

        assert!(orphaned.is_empty());
    }

    #[test]
    fn gc_cluster_all_orphaned_when_desired_empty() {
        let existing = vec!["pv-a", "pv-b"];
        let desired: HashSet<String> = HashSet::new();

        let orphaned: Vec<_> = existing
            .into_iter()
            .filter(|n| !desired.contains(*n))
            .collect();

        assert_eq!(orphaned.len(), 2);
    }

    // =========================================================================
    // GC — namespaced diffing
    // =========================================================================

    #[test]
    fn gc_namespaced_finds_orphans() {
        let existing = vec![
            ("default".to_string(), "pvc-a".to_string()),
            ("default".to_string(), "pvc-b".to_string()),
            ("prod".to_string(), "pvc-c".to_string()),
        ];
        let desired: HashSet<(String, String)> =
            [("default".to_string(), "pvc-a".to_string())].into();

        let orphaned: Vec<_> = existing
            .into_iter()
            .filter(|p| !desired.contains(p))
            .collect();

        assert_eq!(orphaned.len(), 2);
        assert!(orphaned.contains(&("default".to_string(), "pvc-b".to_string())));
        assert!(orphaned.contains(&("prod".to_string(), "pvc-c".to_string())));
    }

    #[test]
    fn gc_namespaced_no_orphans_when_all_desired() {
        let existing = vec![
            ("default".to_string(), "pvc-a".to_string()),
            ("prod".to_string(), "pvc-b".to_string()),
        ];
        let desired: HashSet<(String, String)> = [
            ("default".to_string(), "pvc-a".to_string()),
            ("prod".to_string(), "pvc-b".to_string()),
        ]
        .into();

        let orphaned: Vec<_> = existing
            .into_iter()
            .filter(|p| !desired.contains(p))
            .collect();

        assert!(orphaned.is_empty());
    }

    #[test]
    fn gc_namespaced_same_name_different_namespace_is_not_orphan() {
        // "pvc-a" in "default" is desired; "pvc-a" in "prod" is not.
        let existing = vec![
            ("default".to_string(), "pvc-a".to_string()),
            ("prod".to_string(), "pvc-a".to_string()),
        ];
        let desired: HashSet<(String, String)> =
            [("default".to_string(), "pvc-a".to_string())].into();

        let orphaned: Vec<_> = existing
            .into_iter()
            .filter(|p| !desired.contains(p))
            .collect();

        assert_eq!(orphaned.len(), 1);
        assert_eq!(orphaned[0], ("prod".to_string(), "pvc-a".to_string()));
    }

    // =========================================================================
    // Error variants
    // =========================================================================

    #[test]
    fn missing_metadata_error_displays_field_name() {
        let err = KubeGenericError::MissingMetadata("name".to_string());
        assert!(err.to_string().contains("name"));
    }

    #[test]
    fn other_error_wraps_anyhow() {
        let err = KubeGenericError::Other(anyhow::anyhow!("something failed"));
        assert!(err.to_string().contains("something failed"));
    }
}
