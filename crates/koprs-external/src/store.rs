//! Object store poller.
//!
//! [`ObjectStorePoller`] lists objects under a bucket/prefix on every tick and
//! emits [`ExternalEvent`]s when objects are added, changed (ETag differs), or
//! removed. Requires the `object-store` Cargo feature.
//!
//! The poller accepts any backend that implements the
//! [`object_store::ObjectStore`] trait — S3, GCS, Azure Blob,
//! local filesystem, HTTP, or the built-in in-memory store. Callers build
//! their preferred backend and pass it as `Arc<dyn ObjectStore>`:
//!
//! ```ignore
//! // Requires object_store = { version = "0.11", features = ["aws"] }
//! use std::sync::Arc;
//! use object_store::aws::AmazonS3Builder;
//! use koprs_external::store::ObjectStorePoller;
//!
//! let store = Arc::new(
//!     AmazonS3Builder::from_env()
//!         .with_bucket_name("my-bucket")
//!         .build()
//!         .unwrap(),
//! );
//! let poller = ObjectStorePoller::new(store).with_prefix("configs/");
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use futures::TryStreamExt;
use futures::future::BoxFuture;
use object_store::{ObjectStore, path::Path};
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
// ObjectStorePoller
// ---------------------------------------------------------------------------

/// Polls any [`object_store::ObjectStore`]-compatible backend for
/// object changes.
///
/// On each tick the poller lists all objects under the configured prefix,
/// compares them to the previous listing by ETag, and emits the appropriate
/// events. Objects without an ETag are treated as always-new and produce an
/// [`ExternalEvent::Added`] on every poll until an ETag is available.
///
/// # Examples
///
/// ```ignore
/// // Requires object_store = { version = "0.11", features = ["aws"] }
/// use std::sync::Arc;
/// use std::time::Duration;
/// use object_store::aws::AmazonS3Builder;
/// use koprs_external::store::ObjectStorePoller;
/// use koprs_external::watcher::{watch_external, ExternalEvent};
/// use tokio::sync::mpsc;
///
/// let store = Arc::new(
///     AmazonS3Builder::from_env()
///         .with_bucket_name("my-bucket")
///         .build()
///         .unwrap(),
/// );
///
/// let (tx, mut rx) = mpsc::channel(16);
/// let poller = ObjectStorePoller::new(store).with_prefix("configs/");
/// let _handle = watch_external(poller, Duration::from_secs(60), tx);
///
/// while let Some(event) = rx.recv().await {
///     match event {
///         ExternalEvent::Added(obj)    => println!("new:     {}", obj.path),
///         ExternalEvent::Modified(obj) => println!("changed: {}", obj.path),
///         ExternalEvent::Removed(obj)  => println!("deleted: {}", obj.path),
///     }
/// }
/// ```
pub struct ObjectStorePoller {
    store: Arc<dyn ObjectStore>,
    prefix: Option<Path>,
    name: String,
    // path -> etag — tracks last-seen state for change detection
    known: HashMap<String, String>,
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
            known: HashMap::new(),
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
}

impl ExternalSource for ObjectStorePoller {
    type Item = StoredObject;

    fn name(&self) -> &str {
        &self.name
    }

    fn poll(&mut self) -> BoxFuture<'_, Result<Vec<ExternalEvent<StoredObject>>>> {
        // Clone the Arc and prefix so the async block owns them, avoiding any
        // self-referential borrow across the .await point on the listing stream.
        let store = Arc::clone(&self.store);
        let prefix = self.prefix.clone();
        let this = self;

        Box::pin(async move {
            let stream = store.list(prefix.as_ref());
            let listing: Vec<_> = stream
                .try_collect()
                .await
                .map_err(|e| ExternalError::Internal(format!("object store list failed: {e}")))?;

            let mut current: HashMap<String, StoredObject> = HashMap::new();
            for meta in listing {
                let path = meta.location.to_string();
                current.insert(
                    path.clone(),
                    StoredObject {
                        path,
                        etag: meta.e_tag,
                        size: meta.size,
                    },
                );
            }

            let mut events: Vec<ExternalEvent<StoredObject>> = Vec::new();

            // Added and modified
            for (path, obj) in &current {
                match this.known.get(path) {
                    None => events.push(ExternalEvent::Added(obj.clone())),
                    Some(known_etag) => {
                        if obj.etag.as_deref() != Some(known_etag.as_str()) {
                            events.push(ExternalEvent::Modified(obj.clone()));
                        }
                    }
                }
            }

            // Removed
            for path in this.known.keys() {
                if !current.contains_key(path) {
                    events.push(ExternalEvent::Removed(StoredObject {
                        path: path.clone(),
                        etag: None,
                        size: 0,
                    }));
                }
            }

            // Advance state: only track objects that have an ETag for diffing
            this.known = current
                .into_iter()
                .filter_map(|(k, v)| v.etag.map(|e| (k, e)))
                .collect();

            debug!(
                source = %this.name,
                added    = events.iter().filter(|e| matches!(e, ExternalEvent::Added(_))).count(),
                modified = events.iter().filter(|e| matches!(e, ExternalEvent::Modified(_))).count(),
                removed  = events.iter().filter(|e| matches!(e, ExternalEvent::Removed(_))).count(),
                "Object store poll completed"
            );

            Ok(events)
        })
    }
}
