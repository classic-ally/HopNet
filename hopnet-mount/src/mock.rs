//! Mock NodeTransport: in-memory tree with call recording.
//!
//! Two jobs (RFC-018 testing model): serve a fake namespace so the FUSE
//! layer can be developed and demoed with no node, and record every call
//! the core emits so tests assert behaviour at the boundary. Scriptable
//! failures (dropped watch, 503s) land in S4; the structure allows them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use hopnet_common::CustomUUID;

use crate::transport::{
    BoxFuture, Cursor, Health, Item, ItemId, ItemKind, NodeTransport, Page, StatfsInfo,
    TransportError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallRecord {
    Lookup { parent: ItemId, name: String },
    Item { id: ItemId },
    Enumerate { parent: ItemId, cursor: Option<String> },
    Changes { since: i64 },
    Watch,
    ReadBlob { blob: CustomUUID, offset: u64, len: u64 },
    CreateFolder { parent: ItemId, name: String },
    CreateFile { parent: ItemId, name: String, bytes: Vec<u8> },
    UpdateContent { id: CustomUUID, bytes: Vec<u8> },
    Rename { id: CustomUUID, new_parent: Option<ItemId>, new_name: Option<String> },
    Delete { id: CustomUUID, recursive: bool },
    Health,
    Statfs,
}

struct MockState {
    items: HashMap<ItemId, Item>,
    /// Children ids per folder, unsorted; enumerate orders by id string.
    children: HashMap<ItemId, Vec<ItemId>>,
    calls: Vec<CallRecord>,
    page_size: usize,
    height: i64,
    /// Mutation journal — (height, id, latest state | None=deleted); the
    /// mock's modification_log, driving changes(since).
    journal: Vec<(i64, ItemId, Option<Item>)>,
    /// Live watch connection, if any; dropping it (drop_watch) is the
    /// scripted-disconnect failure mode.
    watch_tx: Option<tokio::sync::mpsc::UnboundedSender<crate::transport::WatchEvent>>,
    /// Blob plaintext store; keyed by blob id. Old blobs stay readable
    /// after content updates (node semantics — snapshot-at-open).
    blobs: HashMap<CustomUUID, Vec<u8>>,
    /// While true, read_blob calls park on the gate (deterministic
    /// single-flight tests).
    fetch_hold: tokio::sync::watch::Sender<bool>,
    /// While true, content uploads (create_file/update_content) park —
    /// the release-vs-fsync tier tests.
    upload_hold: tokio::sync::watch::Sender<bool>,
    /// When true, content uploads fail with Unavailable.
    upload_fail: bool,
    /// Scripted statfs numbers; None = statfs fails Unavailable (the
    /// node-unreachable arm of the daemon's last-known-value cache).
    statfs: Option<StatfsInfo>,
}

impl MockState {
    fn root_item(&self) -> Item {
        Item {
            id: ItemId::Root,
            parent: ItemId::Root,
            name: String::new(),
            kind: ItemKind::Folder,
            created: SystemTime::UNIX_EPOCH,
            modified: SystemTime::UNIX_EPOCH,
            height: self.height,
            blob: None,
        }
    }
}

pub struct MockTransport {
    state: Arc<Mutex<MockState>>,
}

/// Test-side handle: mutate the tree and inspect recorded calls without
/// going through the transport trait.
pub struct MockHandle {
    state: Arc<Mutex<MockState>>,
}

impl MockTransport {
    pub fn new() -> (Arc<Self>, MockHandle) {
        let state = Arc::new(Mutex::new(MockState {
            items: HashMap::new(),
            children: HashMap::from([(ItemId::Root, Vec::new())]),
            calls: Vec::new(),
            page_size: 100,
            height: 1,
            journal: Vec::new(),
            watch_tx: None,
            blobs: HashMap::new(),
            fetch_hold: tokio::sync::watch::channel(false).0,
            upload_hold: tokio::sync::watch::channel(false).0,
            upload_fail: false,
            statfs: Some(StatfsInfo {
                total_bytes: 100 * 1024 * 1024 * 1024,
                used_bytes: 25 * 1024 * 1024 * 1024,
            }),
        }));
        (
            Arc::new(MockTransport {
                state: state.clone(),
            }),
            MockHandle { state },
        )
    }

    /// A small fixed tree for the `--mock` demo mount.
    pub fn with_demo_tree() -> Arc<Self> {
        let (transport, handle) = Self::new();
        let docs = handle.add_folder(ItemId::Root, "Documents");
        let rfcs = handle.add_folder(docs.clone(), "RFCs");
        handle.add_file(rfcs, "rfc-018.md", 12_400);
        handle.add_file(docs.clone(), "Notes.txt", 640);
        let photos = handle.add_folder(ItemId::Root, "Photos");
        handle.add_file(photos, "trip.jpg", 4_812_332);
        handle.add_file(ItemId::Root, "README.md", 2_048);
        transport
    }
}

impl MockHandle {
    fn insert(&self, parent: ItemId, name: &str, kind: ItemKind) -> ItemId {
        let mut state = self.state.lock().expect("mock poisoned");
        let id = ItemId::Inode(CustomUUID::new(None));
        state.height += 1;
        let blob = match kind {
            // Non-empty mock files get a fake backing blob id, mirroring
            // the node (empty files and folders have none).
            ItemKind::File { size } if size > 0 => Some(CustomUUID::new(None)),
            _ => None,
        };
        let item = Item {
            id: id.clone(),
            parent: parent.clone(),
            name: name.to_string(),
            kind: kind.clone(),
            created: SystemTime::now(),
            modified: SystemTime::now(),
            height: state.height,
            blob,
        };
        state.items.insert(id.clone(), item.clone());
        state.children.entry(parent).or_default().push(id.clone());
        if matches!(kind, ItemKind::Folder) {
            state.children.entry(id.clone()).or_default();
        }
        let height = state.height;
        state.journal.push((height, id.clone(), Some(item)));
        id
    }

    pub fn add_folder(&self, parent: ItemId, name: &str) -> ItemId {
        self.insert(parent, name, ItemKind::Folder)
    }

    pub fn add_file(&self, parent: ItemId, name: &str, size: u64) -> ItemId {
        self.insert(parent, name, ItemKind::File { size })
    }

    pub fn remove(&self, id: &ItemId) {
        let mut state = self.state.lock().expect("mock poisoned");
        state.height += 1;
        if let Some(item) = state.items.remove(id) {
            if let Some(siblings) = state.children.get_mut(&item.parent) {
                siblings.retain(|c| c != id);
            }
        }
        state.children.remove(id);
        let height = state.height;
        state.journal.push((height, id.clone(), None));
    }

    /// Remote content modification: new size (and height bump + journal).
    pub fn update_file_size(&self, id: &ItemId, size: u64) {
        let mut state = self.state.lock().expect("mock poisoned");
        state.height += 1;
        let height = state.height;
        if let Some(item) = state.items.get_mut(id) {
            item.kind = ItemKind::File { size };
            item.height = height;
            let snapshot = item.clone();
            state.journal.push((height, id.clone(), Some(snapshot)));
        }
    }

    /// Remote rename/move.
    pub fn rename(&self, id: &ItemId, new_parent: ItemId, new_name: &str) {
        let mut state = self.state.lock().expect("mock poisoned");
        state.height += 1;
        let height = state.height;
        let Some(mut item) = state.items.remove(id) else {
            return;
        };
        if let Some(siblings) = state.children.get_mut(&item.parent) {
            siblings.retain(|c| c != id);
        }
        item.parent = new_parent.clone();
        item.name = new_name.to_string();
        item.height = height;
        state.items.insert(id.clone(), item.clone());
        state.children.entry(new_parent).or_default().push(id.clone());
        state.journal.push((height, id.clone(), Some(item)));
    }

    /// Send a content-free poke on the live watch connection.
    pub fn poke(&self) {
        let state = self.state.lock().expect("mock poisoned");
        if let Some(tx) = &state.watch_tx {
            let _ = tx.send(crate::transport::WatchEvent::Poke);
        }
    }

    /// Scripted disconnect: drop the live watch connection's sender — the
    /// daemon's stream ends as if the node vanished.
    pub fn drop_watch(&self) {
        self.state.lock().expect("mock poisoned").watch_tx = None;
    }

    /// Whether a watch connection is currently held open.
    pub fn watch_connected(&self) -> bool {
        self.state.lock().expect("mock poisoned").watch_tx.is_some()
    }

    /// A file whose content the mock can serve via read_blob.
    pub fn add_file_with_content(
        &self,
        parent: ItemId,
        name: &str,
        content: &[u8],
    ) -> (ItemId, CustomUUID) {
        let id = self.insert(parent, name, ItemKind::File { size: content.len() as u64 });
        let mut state = self.state.lock().expect("mock poisoned");
        let blob = match state.items.get(&id).and_then(|i| i.blob.clone()) {
            Some(blob) => blob,
            None => {
                // Zero-length content still needs no blob (node semantics).
                let blob = CustomUUID::new(None);
                if let Some(item) = state.items.get_mut(&id) {
                    item.blob = Some(blob.clone());
                }
                blob
            }
        };
        state.blobs.insert(blob.clone(), content.to_vec());
        (id, blob)
    }

    /// Remote content replacement: mints a NEW blob id (node modify
    /// semantics); the old blob's content stays readable so open handles
    /// keep their snapshot.
    pub fn update_file_content(&self, id: &ItemId, content: &[u8]) -> CustomUUID {
        let mut state = self.state.lock().expect("mock poisoned");
        state.height += 1;
        let height = state.height;
        let new_blob = CustomUUID::new(None);
        state.blobs.insert(new_blob.clone(), content.to_vec());
        if let Some(item) = state.items.get_mut(id) {
            item.kind = ItemKind::File { size: content.len() as u64 };
            item.blob = Some(new_blob.clone());
            item.height = height;
            let snapshot = item.clone();
            state.journal.push((height, id.clone(), Some(snapshot)));
        }
        new_blob
    }

    /// Delete a blob's content outright (scripted GC race).
    pub fn drop_blob(&self, blob: &CustomUUID) {
        self.state.lock().expect("mock poisoned").blobs.remove(blob);
    }

    /// Park subsequent read_blob calls until release_fetches.
    pub fn hold_fetches(&self) {
        let _ = self
            .state
            .lock()
            .expect("mock poisoned")
            .fetch_hold
            .send_replace(true);
    }

    pub fn release_fetches(&self) {
        let _ = self
            .state
            .lock()
            .expect("mock poisoned")
            .fetch_hold
            .send_replace(false);
    }

    /// Park content uploads until release_uploads (fsync-tier tests).
    pub fn hold_uploads(&self) {
        let _ = self
            .state
            .lock()
            .expect("mock poisoned")
            .upload_hold
            .send_replace(true);
    }

    pub fn release_uploads(&self) {
        let _ = self
            .state
            .lock()
            .expect("mock poisoned")
            .upload_hold
            .send_replace(false);
    }

    /// Make content uploads fail (retry/recovery tests).
    pub fn set_upload_fail(&self, fail: bool) {
        self.state.lock().expect("mock poisoned").upload_fail = fail;
    }

    /// Script the statfs numbers; None makes statfs fail Unavailable.
    pub fn set_statfs(&self, info: Option<StatfsInfo>) {
        self.state.lock().expect("mock poisoned").statfs = info;
    }

    /// Page size for enumerate — small values force multi-page listings.
    pub fn set_page_size(&self, n: usize) {
        self.state.lock().expect("mock poisoned").page_size = n.max(1);
    }

    pub fn calls(&self) -> Vec<CallRecord> {
        self.state.lock().expect("mock poisoned").calls.clone()
    }

    pub fn clear_calls(&self) {
        self.state.lock().expect("mock poisoned").calls.clear();
    }
}

impl NodeTransport for MockTransport {
    fn lookup(
        &self,
        parent: ItemId,
        name: String,
    ) -> BoxFuture<'_, Result<Option<Item>, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state.lock().expect("mock poisoned");
            state.calls.push(CallRecord::Lookup {
                parent: parent.clone(),
                name: name.clone(),
            });
            let hit = state
                .children
                .get(&parent)
                .into_iter()
                .flatten()
                .filter_map(|id| state.items.get(id))
                .find(|item| item.name == name)
                .cloned();
            Ok(hit)
        })
    }

    fn item(&self, id: ItemId) -> BoxFuture<'_, Result<Option<Item>, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state.lock().expect("mock poisoned");
            state.calls.push(CallRecord::Item { id: id.clone() });
            if id == ItemId::Root {
                let root = state.root_item();
                return Ok(Some(root));
            }
            Ok(state.items.get(&id).cloned())
        })
    }

    fn enumerate(
        &self,
        parent: ItemId,
        cursor: Option<Cursor>,
    ) -> BoxFuture<'_, Result<Page, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state.lock().expect("mock poisoned");
            state.calls.push(CallRecord::Enumerate {
                parent: parent.clone(),
                cursor: cursor.as_ref().map(|c| c.0.clone()),
            });

            // Stable order by id string; cursor = last-seen id (RFC-018).
            let mut ids: Vec<String> = state
                .children
                .get(&parent)
                .into_iter()
                .flatten()
                .map(|id| match id {
                    ItemId::Inode(u) => u.to_string(),
                    ItemId::Root => unreachable!("root is never a child"),
                })
                .collect();
            ids.sort();

            let after = cursor.map(|c| c.0);
            let page_size = state.page_size;
            let page_ids: Vec<String> = ids
                .into_iter()
                .filter(|id| after.as_ref().is_none_or(|a| id > a))
                .take(page_size)
                .collect();

            let items: Vec<Item> = page_ids
                .iter()
                .filter_map(|s| {
                    let uuid: CustomUUID = s.parse().ok()?;
                    state.items.get(&ItemId::Inode(uuid)).cloned()
                })
                .collect();
            let next = if items.len() == page_size {
                page_ids.last().map(|s| Cursor(s.clone()))
            } else {
                None
            };
            Ok(Page { items, next })
        })
    }

    fn changes(
        &self,
        since: crate::transport::Height,
    ) -> BoxFuture<'_, Result<crate::transport::Changes, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state.lock().expect("mock poisoned");
            state.calls.push(CallRecord::Changes { since });

            // Latest journal entry per id, strictly after `since` — the
            // same semantics as the node's modification_log query.
            let mut latest: HashMap<ItemId, (i64, Option<Item>)> = HashMap::new();
            for (height, id, item) in state.journal.iter().filter(|(h, _, _)| *h > since) {
                let entry = latest.entry(id.clone()).or_insert((*height, item.clone()));
                if *height >= entry.0 {
                    *entry = (*height, item.clone());
                }
            }
            let mut items = Vec::new();
            let mut deleted = Vec::new();
            for (id, (_, item)) in latest {
                match item {
                    Some(item) => items.push(item),
                    None => {
                        if let ItemId::Inode(uuid) = id {
                            deleted.push(uuid);
                        }
                    }
                }
            }
            Ok(crate::transport::Changes {
                items,
                deleted,
                height: state.height,
            })
        })
    }

    fn watch(
        &self,
    ) -> BoxFuture<'_, Result<crate::transport::WatchStream, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut state = state.lock().expect("mock poisoned");
            state.calls.push(CallRecord::Watch);
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            state.watch_tx = Some(tx);
            Ok(Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(rx))
                as crate::transport::WatchStream)
        })
    }

    fn read_blob(
        &self,
        blob: CustomUUID,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'_, Result<Vec<u8>, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut gate = {
                let mut locked = state.lock().expect("mock poisoned");
                locked.calls.push(CallRecord::ReadBlob {
                    blob: blob.clone(),
                    offset,
                    len,
                });
                locked.fetch_hold.subscribe()
            };
            // Park while held (lock released above — mutations proceed).
            let _ = gate.wait_for(|held| !held).await;

            let locked = state.lock().expect("mock poisoned");
            let Some(content) = locked.blobs.get(&blob) else {
                return Err(TransportError::Protocol("blob gone".to_string()));
            };
            let start = (offset as usize).min(content.len());
            let end = ((offset + len) as usize).min(content.len());
            Ok(content[start..end].to_vec())
        })
    }

    fn create_folder(
        &self,
        parent: ItemId,
        name: String,
    ) -> BoxFuture<'_, Result<crate::transport::Mutated, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            {
                let mut locked = state.lock().expect("mock poisoned");
                locked.calls.push(CallRecord::CreateFolder {
                    parent: parent.clone(),
                    name: name.clone(),
                });
                if child_by_name(&locked, &parent, &name).is_some() {
                    return Err(TransportError::Conflict);
                }
            }
            let handle = MockHandle {
                state: state.clone(),
            };
            let id = handle.add_folder(parent, &name);
            let locked = state.lock().expect("mock poisoned");
            Ok(crate::transport::Mutated {
                item: locked.items.get(&id).cloned(),
                height: locked.height,
            })
        })
    }

    fn create_file(
        &self,
        parent: ItemId,
        name: String,
        size: u64,
        content: crate::transport::ByteSource,
    ) -> BoxFuture<'_, Result<crate::transport::Mutated, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let bytes = collect_source(content).await?;
            if bytes.len() as u64 != size {
                return Err(TransportError::Protocol("size mismatch".to_string()));
            }
            {
                let mut locked = state.lock().expect("mock poisoned");
                locked.calls.push(CallRecord::CreateFile {
                    parent: parent.clone(),
                    name: name.clone(),
                    bytes: bytes.clone(),
                });
                if child_by_name(&locked, &parent, &name).is_some() {
                    return Err(TransportError::Conflict);
                }
            }
            let handle = MockHandle {
                state: state.clone(),
            };
            let (id, _blob) = handle.add_file_with_content(parent, &name, &bytes);
            let locked = state.lock().expect("mock poisoned");
            Ok(crate::transport::Mutated {
                item: locked.items.get(&id).cloned(),
                height: locked.height,
            })
        })
    }

    fn update_content(
        &self,
        id: CustomUUID,
        size: u64,
        content: crate::transport::ByteSource,
    ) -> BoxFuture<'_, Result<crate::transport::Mutated, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let bytes = collect_source(content).await?;
            if bytes.len() as u64 != size {
                return Err(TransportError::Protocol("size mismatch".to_string()));
            }
            let mut gate = {
                let mut locked = state.lock().expect("mock poisoned");
                locked.calls.push(CallRecord::UpdateContent {
                    id: id.clone(),
                    bytes: bytes.clone(),
                });
                if locked.upload_fail {
                    return Err(TransportError::Unavailable("scripted failure".to_string()));
                }
                locked.upload_hold.subscribe()
            };
            let _ = gate.wait_for(|held| !held).await;

            let item_id = ItemId::Inode(id);
            {
                let locked = state.lock().expect("mock poisoned");
                if !locked.items.contains_key(&item_id) {
                    return Err(TransportError::Protocol("item gone".to_string()));
                }
            }
            let handle = MockHandle {
                state: state.clone(),
            };
            handle.update_file_content(&item_id, &bytes);
            let locked = state.lock().expect("mock poisoned");
            Ok(crate::transport::Mutated {
                item: locked.items.get(&item_id).cloned(),
                height: locked.height,
            })
        })
    }

    fn rename(
        &self,
        id: CustomUUID,
        new_parent: Option<ItemId>,
        new_name: Option<String>,
    ) -> BoxFuture<'_, Result<crate::transport::Mutated, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let item_id = ItemId::Inode(id.clone());
            let (target_parent, target_name) = {
                let mut locked = state.lock().expect("mock poisoned");
                locked.calls.push(CallRecord::Rename {
                    id,
                    new_parent: new_parent.clone(),
                    new_name: new_name.clone(),
                });
                let Some(current) = locked.items.get(&item_id) else {
                    return Err(TransportError::Protocol("item gone".to_string()));
                };
                let target_parent = new_parent.unwrap_or_else(|| current.parent.clone());
                let target_name = new_name.unwrap_or_else(|| current.name.clone());
                if let Some(existing) = child_by_name(&locked, &target_parent, &target_name) {
                    if existing != item_id {
                        return Err(TransportError::Conflict);
                    }
                }
                (target_parent, target_name)
            };
            let handle = MockHandle {
                state: state.clone(),
            };
            handle.rename(&item_id, target_parent, &target_name);
            let locked = state.lock().expect("mock poisoned");
            Ok(crate::transport::Mutated {
                item: locked.items.get(&item_id).cloned(),
                height: locked.height,
            })
        })
    }

    fn delete(
        &self,
        id: CustomUUID,
        recursive: bool,
    ) -> BoxFuture<'_, Result<crate::transport::Height, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let item_id = ItemId::Inode(id.clone());
            {
                let mut locked = state.lock().expect("mock poisoned");
                locked.calls.push(CallRecord::Delete { id, recursive });
                if !locked.items.contains_key(&item_id) {
                    return Err(TransportError::Protocol("item gone".to_string()));
                }
                let has_children = locked
                    .children
                    .get(&item_id)
                    .is_some_and(|c| !c.is_empty());
                if has_children && !recursive {
                    return Err(TransportError::Conflict);
                }
            }
            let handle = MockHandle {
                state: state.clone(),
            };
            handle.remove(&item_id);
            Ok(state.lock().expect("mock poisoned").height)
        })
    }

    fn health(&self) -> BoxFuture<'_, Result<Health, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            state
                .lock()
                .expect("mock poisoned")
                .calls
                .push(CallRecord::Health);
            Ok(Health::Ready)
        })
    }

    fn statfs(&self) -> BoxFuture<'_, Result<StatfsInfo, TransportError>> {
        let state = self.state.clone();
        Box::pin(async move {
            let mut locked = state.lock().expect("mock poisoned");
            locked.calls.push(CallRecord::Statfs);
            match locked.statfs {
                Some(info) => Ok(info),
                None => Err(TransportError::Unavailable("statfs scripted away".into())),
            }
        })
    }
}

fn child_by_name(state: &MockState, parent: &ItemId, name: &str) -> Option<ItemId> {
    state
        .children
        .get(parent)?
        .iter()
        .find(|id| state.items.get(id).is_some_and(|i| i.name == name))
        .cloned()
}

async fn collect_source(
    mut source: crate::transport::ByteSource,
) -> Result<Vec<u8>, TransportError> {
    use tokio_stream::StreamExt;
    let mut bytes = Vec::new();
    while let Some(chunk) = source.next().await {
        let chunk = chunk.map_err(|e| TransportError::Protocol(e.to_string()))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
