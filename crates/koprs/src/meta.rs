//! Fluent builder for [`ObjectMeta`].

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::core::ObjectMeta;
use std::collections::BTreeMap;

/// Fluent builder for [`ObjectMeta`].
///
/// Constructs the metadata block for any Kubernetes object. Each setter
/// returns `self` for chaining; call [`build`][ObjectMetaBuilder::build]
/// to produce the final [`ObjectMeta`].
///
/// # Examples
///
/// ```
/// use koprs::meta::ObjectMetaBuilder;
///
/// let meta = ObjectMetaBuilder::new()
///     .name("my-configmap")
///     .namespace("my-namespace")
///     .label("app.kubernetes.io/managed-by", "my-operator")
///     .label("my-operator/owner", "my-cr")
///     .build();
///
/// assert_eq!(meta.name.as_deref(), Some("my-configmap"));
/// assert_eq!(meta.namespace.as_deref(), Some("my-namespace"));
/// assert_eq!(meta.labels.as_ref().unwrap().len(), 2);
/// ```
#[derive(Default)]
pub struct ObjectMetaBuilder {
    name: Option<String>,
    namespace: Option<String>,
    labels: BTreeMap<String, String>,
    annotations: BTreeMap<String, String>,
    owner_references: Vec<OwnerReference>,
}

impl ObjectMetaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Add a single label.
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.insert(key.into(), value.into());
        self
    }

    /// Add multiple labels from an iterable of `(key, value)` pairs.
    pub fn labels(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (k, v) in pairs {
            self.labels.insert(k.into(), v.into());
        }
        self
    }

    /// Add a single annotation.
    pub fn annotation(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.annotations.insert(key.into(), value.into());
        self
    }

    /// Add an owner reference (e.g. from [`koprs::owners::owner_ref`]).
    pub fn owner_ref(mut self, owner_ref: OwnerReference) -> Self {
        self.owner_references.push(owner_ref);
        self
    }

    /// Consume the builder and produce an [`ObjectMeta`].
    pub fn build(self) -> ObjectMeta {
        ObjectMeta {
            name: self.name,
            namespace: self.namespace,
            labels: if self.labels.is_empty() {
                None
            } else {
                Some(self.labels)
            },
            annotations: if self.annotations.is_empty() {
                None
            } else {
                Some(self.annotations)
            },
            owner_references: if self.owner_references.is_empty() {
                None
            } else {
                Some(self.owner_references)
            },
            ..Default::default()
        }
    }
}
