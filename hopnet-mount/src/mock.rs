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
    BoxFuture, Cursor, Health, Item, ItemId, ItemKind, NodeTransport, Page, TransportError,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallRecord {
    Lookup { parent: ItemId, name: String },
    Item { id: ItemId },
    Enumerate { parent: ItemId, cursor: Option<String> },
    Health,
}

struct MockState {
    items: HashMap<ItemId, Item>,
    /// Children ids per folder, unsorted; enumerate orders by id string.
    children: HashMap<ItemId, Vec<ItemId>>,
    calls: Vec<CallRecord>,
    page_size: usize,
    height: i64,
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
        let item = Item {
            id: id.clone(),
            parent: parent.clone(),
            name: name.to_string(),
            kind: kind.clone(),
            created: SystemTime::now(),
            modified: SystemTime::now(),
            height: state.height,
        };
        state.items.insert(id.clone(), item);
        state.children.entry(parent).or_default().push(id.clone());
        if matches!(kind, ItemKind::Folder) {
            state.children.entry(id.clone()).or_default();
        }
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
}
