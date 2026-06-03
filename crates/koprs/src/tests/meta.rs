// src/tests/meta.rs

#[cfg(test)]
mod meta_tests {
    use crate::meta::ObjectMetaBuilder;

    // -----------------------------------------------------------------------
    // name / namespace
    // -----------------------------------------------------------------------

    #[test]
    fn builder_sets_name() {
        let meta = ObjectMetaBuilder::new().name("my-resource").build();
        assert_eq!(meta.name.as_deref(), Some("my-resource"));
    }

    #[test]
    fn builder_sets_namespace() {
        let meta = ObjectMetaBuilder::new().namespace("my-ns").build();
        assert_eq!(meta.namespace.as_deref(), Some("my-ns"));
    }

    #[test]
    fn builder_name_and_namespace_together() {
        let meta = ObjectMetaBuilder::new().name("res").namespace("ns").build();
        assert_eq!(meta.name.as_deref(), Some("res"));
        assert_eq!(meta.namespace.as_deref(), Some("ns"));
    }

    #[test]
    fn builder_with_no_name_leaves_name_none() {
        let meta = ObjectMetaBuilder::new().namespace("ns").build();
        assert!(meta.name.is_none());
    }

    // -----------------------------------------------------------------------
    // labels
    // -----------------------------------------------------------------------

    #[test]
    fn builder_adds_single_label() {
        let meta = ObjectMetaBuilder::new()
            .label("app.kubernetes.io/managed-by", "my-op")
            .build();
        let labels = meta.labels.unwrap();
        assert_eq!(
            labels
                .get("app.kubernetes.io/managed-by")
                .map(|s| s.as_str()),
            Some("my-op")
        );
    }

    #[test]
    fn builder_adds_multiple_labels_via_label() {
        let meta = ObjectMetaBuilder::new()
            .label("k1", "v1")
            .label("k2", "v2")
            .build();
        let labels = meta.labels.unwrap();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels["k1"], "v1");
        assert_eq!(labels["k2"], "v2");
    }

    #[test]
    fn builder_adds_labels_via_labels_batch() {
        let meta = ObjectMetaBuilder::new()
            .labels([("k1", "v1"), ("k2", "v2")])
            .build();
        let labels = meta.labels.unwrap();
        assert_eq!(labels.len(), 2);
        assert_eq!(labels["k1"], "v1");
        assert_eq!(labels["k2"], "v2");
    }

    #[test]
    fn builder_label_overwrites_duplicate_key() {
        let meta = ObjectMetaBuilder::new()
            .label("k", "first")
            .label("k", "second")
            .build();
        let labels = meta.labels.unwrap();
        assert_eq!(labels.len(), 1);
        assert_eq!(labels["k"], "second");
    }

    #[test]
    fn builder_with_no_labels_leaves_labels_none() {
        let meta = ObjectMetaBuilder::new().name("res").build();
        assert!(meta.labels.is_none());
    }

    // -----------------------------------------------------------------------
    // annotations
    // -----------------------------------------------------------------------

    #[test]
    fn builder_adds_annotation() {
        let meta = ObjectMetaBuilder::new()
            .annotation("my-op/last-synced", "2024-01-01")
            .build();
        let annotations = meta.annotations.unwrap();
        assert_eq!(annotations["my-op/last-synced"], "2024-01-01");
    }

    #[test]
    fn builder_with_no_annotations_leaves_annotations_none() {
        let meta = ObjectMetaBuilder::new().name("res").build();
        assert!(meta.annotations.is_none());
    }

    // -----------------------------------------------------------------------
    // owner references
    // -----------------------------------------------------------------------

    #[test]
    fn builder_with_no_owner_refs_leaves_owner_references_none() {
        let meta = ObjectMetaBuilder::new().name("res").build();
        assert!(meta.owner_references.is_none());
    }

    #[test]
    fn builder_adds_owner_ref() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
        let owner = OwnerReference {
            api_version: "v1".to_string(),
            kind: "ConfigMap".to_string(),
            name: "parent".to_string(),
            uid: "abc-123".to_string(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        };
        let meta = ObjectMetaBuilder::new().owner_ref(owner.clone()).build();
        let refs = meta.owner_references.unwrap();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].name, "parent");
        assert_eq!(refs[0].uid, "abc-123");
    }

    // -----------------------------------------------------------------------
    // default / empty builder
    // -----------------------------------------------------------------------

    #[test]
    fn default_builder_produces_all_none_meta() {
        let meta = ObjectMetaBuilder::default().build();
        assert!(meta.name.is_none());
        assert!(meta.namespace.is_none());
        assert!(meta.labels.is_none());
        assert!(meta.annotations.is_none());
        assert!(meta.owner_references.is_none());
    }
}
