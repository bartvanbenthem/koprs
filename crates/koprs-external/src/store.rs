//! Object store poller.
//!
//! [`ObjectStorePoller`] lists objects under a bucket/prefix on every tick and
//! emits [`ExternalEvent`]s when objects are added, changed, or removed.
//! A change is a different ETag, or, for backends that report no ETag, a
//! different modification time or size. Requires the `object-store` Cargo
//! feature.
//!
//! The poller accepts any backend that implements the
//! [`object_store::ObjectStore`] trait — S3, GCS, Azure Blob,
//! local filesystem, HTTP, or the built-in in-memory store. Callers build
//! their preferred backend and pass it as `Arc<dyn ObjectStore>`. See the
//! [crate README](https://docs.rs/koprs-external) for backend-specific examples.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore, path::Path};
use tracing::debug;

use crate::{
    error::{ExternalError, Result},
    watcher::{ExternalEvent, ExternalSource},
};

// ---------------------------------------------------------------------------
// StoredObject
// ---------------------------------------------------------------------------

/// Metadata for a single object returned by [`ObjectStorePoller`].
#[derive(Debug, Clone)]
pub struct StoredObject {
    /// Object path within the store (equivalent to an S3 key).
    pub path: String,
    /// ETag of the object as returned by the store, if available.
    pub etag: Option<String>,
    /// Object size in bytes.
    pub size: usize,
}

// ---------------------------------------------------------------------------
// ObjectVersion
// ---------------------------------------------------------------------------

/// What tells two versions of the same object apart between polls.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ObjectVersion {
    /// The store's own content identifier.
    ETag(String),
    /// Fallback for backends that report no ETag.
    Stat {
        /// Modification time in nanoseconds since the Unix epoch, when it fits.
        modified_unix_nanos: Option<i64>,
        size: usize,
    },
}

impl ObjectVersion {
    fn of(meta: &ObjectMeta) -> Self {
        match &meta.e_tag {
            Some(etag) => Self::ETag(etag.clone()),
            None => Self::Stat {
                modified_unix_nanos: meta.last_modified.timestamp_nanos_opt(),
                size: meta.size,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// ObjectStorePoller
// ---------------------------------------------------------------------------

/// Polls any [`object_store::ObjectStore`]-compatible backend for object changes.
///
/// On each tick the poller lists all objects under the configured prefix,
/// compares them to the previous listing, and emits the appropriate events,
/// ordered by path: additions and modifications first, then removals.
///
/// Objects are compared by ETag. For backends that report no ETag the
/// modification time and size are compared instead, so such objects are
/// reported once when they appear, again only when they change, and as
/// removed when they disappear.
///
/// Backend-specific setup (S3, GCS, Azure) is done by the caller — see the
/// [crate README](https://docs.rs/koprs-external) for examples.
pub struct ObjectStorePoller {
    store: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    name: String,
    /// Last-seen version of every object, by path. Sorted so events come out
    /// in a stable order.
    known: BTreeMap<String, ObjectVersion>,
}

impl ObjectStorePoller {
    /// Create a new poller backed by `store`, watching the root of the store.
    ///
    /// Call [`with_prefix`][Self::with_prefix] to narrow the listing to a
    /// specific path prefix.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            name: store.to_string(),
            store,
            prefix: None,
            known: BTreeMap::new(),
        }
    }

    /// Narrow the listing to paths that begin with `prefix`.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        self.name = format!("{}/{}", self.name.trim_end_matches('/'), prefix);
        self.prefix = Some(Path::from(prefix.as_str()));
        self
    }

    /// Override the display name used in log output.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Compare `listing` with the previous one, return the differences and
    /// remember `listing` for the next poll.
    pub(crate) fn diff_listing(
        &mut self,
        listing: Vec<ObjectMeta>,
    ) -> Vec<ExternalEvent<StoredObject>> {
        let mut current: BTreeMap<String, ObjectVersion> = BTreeMap::new();
        let mut events = Vec::new();

        for meta in listing {
            let path = meta.location.to_string();
            let version = ObjectVersion::of(&meta);
            let object = StoredObject {
                path: path.clone(),
                etag: meta.e_tag,
                size: meta.size,
            };
            match self.known.get(&path) {
                None => events.push(ExternalEvent::Added(object)),
                Some(known_version) if *known_version != version => {
                    events.push(ExternalEvent::Modified(object));
                }
                Some(_unchanged) => {}
            }
            current.insert(path, version);
        }

        for path in self.known.keys() {
            if !current.contains_key(path) {
                events.push(ExternalEvent::Removed(StoredObject {
                    path: path.clone(),
                    etag: None,
                    size: 0,
                }));
            }
        }

        self.known = current;
        events
    }

    async fn poll_once(&mut self) -> Result<Vec<ExternalEvent<StoredObject>>> {
        let mut listing: Vec<ObjectMeta> = self
            .store
            .list(self.prefix.as_ref())
            .try_collect()
            .await
            .map_err(|e| ExternalError::Internal(format!("object store list failed: {e}")))?;
        listing.sort_by(|left, right| left.location.cmp(&right.location));

        let events = self.diff_listing(listing);
        debug!(
            source = %self.name,
            events = events.len(),
            objects = self.known.len(),
            "Object store poll completed"
        );
        Ok(events)
    }
}

impl ExternalSource for ObjectStorePoller {
    type Item = StoredObject;

    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<StoredObject>>>> {
        Box::pin(self.poll_once())
    }
}
