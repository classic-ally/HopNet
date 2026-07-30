//! HTTP implementation of `NodeTransport` (RFC-018 S3) against the node's
//! `/api/integrations/mount` surface (S2).
//!
//! Auth is an RFC-012 device token (`Bearer {device_id}.{secret_hex}`) —
//! bootstrap sessions mean no prior login is needed. Failure discipline
//! per the RFC: one bounded retry on connection errors, then a loud
//! `Unavailable` — never an indefinite hang.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hopnet_common::db::InodeType;
use hopnet_common::fileprovider::{HealthResponse, HealthStatus};
use hopnet_common::mount::{MountEnumerateResponse, MountItem};

use crate::transport::{
    BoxFuture, Cursor, Health, Item, ItemId, ItemKind, NodeTransport, Page, TransportError,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpTransport {
    client: reqwest::Client,
    base: String,
    token: String,
}

impl HttpTransport {
    pub fn new(base_url: &str, token: &str) -> Result<Self, TransportError> {
        let client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| TransportError::Protocol(e.to_string()))?;
        Ok(HttpTransport {
            client,
            base: base_url.trim_end_matches('/').to_string(),
            token: token.to_string(),
        })
    }

    fn url(&self, route: &str) -> String {
        format!("{}/api/integrations/mount/{}", self.base, route)
    }

    /// Authed GET with one retry on connection errors only.
    async fn get_authed(
        &self,
        route: &str,
        query: &[(&str, String)],
    ) -> Result<reqwest::Response, TransportError> {
        for attempt in 0..2 {
            let result = self
                .client
                .get(self.url(route))
                .bearer_auth(&self.token)
                .query(query)
                .send()
                .await;
            match result {
                Ok(response) => return check_status(response),
                Err(e) if is_transport_level(&e) && attempt == 0 => continue,
                Err(e) => return Err(map_reqwest_err(e)),
            }
        }
        unreachable!("loop returns on second attempt")
    }
}

/// Send-level failure: the node is unreachable or the connection died —
/// as opposed to a response we couldn't make sense of. reqwest doesn't
/// always surface refused connections via is_connect(), so any statusless
/// non-decode send error counts.
fn is_transport_level(e: &reqwest::Error) -> bool {
    e.is_connect() || e.is_timeout() || (e.status().is_none() && !e.is_decode() && !e.is_builder())
}

fn map_reqwest_err(e: reqwest::Error) -> TransportError {
    if is_transport_level(&e) {
        TransportError::Unavailable(e.to_string())
    } else {
        TransportError::Protocol(e.to_string())
    }
}

/// 401/428 → Unauthorized; other non-success (except 404, which callers
/// handle as Ok(None)) → Protocol.
fn check_status(response: reqwest::Response) -> Result<reqwest::Response, TransportError> {
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED
        || status == reqwest::StatusCode::PRECONDITION_REQUIRED
    {
        return Err(TransportError::Unauthorized);
    }
    Ok(response)
}

fn ms_to_system_time(ms: i64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms.max(0) as u64)
}

pub(crate) fn item_from_wire(wire: MountItem) -> Item {
    let id = match wire.id {
        Some(uuid) => ItemId::Inode(uuid),
        None => ItemId::Root,
    };
    let parent = match wire.parent_id {
        Some(uuid) => ItemId::Inode(uuid),
        None => ItemId::Root,
    };
    let kind = match wire.item_type {
        InodeType::Folder => ItemKind::Folder,
        InodeType::File => ItemKind::File {
            size: wire.size.unwrap_or(0),
        },
    };
    let created = ms_to_system_time(wire.created_ms);
    Item {
        id,
        parent,
        name: wire.name,
        kind,
        created,
        modified: wire.modified_ms.map(ms_to_system_time).unwrap_or(created),
        height: wire.height.map(i64::from).unwrap_or(0),
        blob: wire.blob_id,
    }
}

async fn parse_json<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, TransportError> {
    let status = response.status();
    if !status.is_success() {
        return Err(TransportError::Protocol(format!(
            "unexpected status {status}"
        )));
    }
    response
        .json::<T>()
        .await
        .map_err(|e| TransportError::Protocol(e.to_string()))
}

impl NodeTransport for HttpTransport {
    fn lookup(
        &self,
        parent: ItemId,
        name: String,
    ) -> BoxFuture<'_, Result<Option<Item>, TransportError>> {
        Box::pin(async move {
            let mut query: Vec<(&str, String)> = vec![("name", name)];
            if let ItemId::Inode(uuid) = &parent {
                query.push(("parent_id", uuid.to_string()));
            }
            let response = self.get_authed("lookup", &query).await?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            Ok(Some(item_from_wire(parse_json::<MountItem>(response).await?)))
        })
    }

    fn item(&self, id: ItemId) -> BoxFuture<'_, Result<Option<Item>, TransportError>> {
        Box::pin(async move {
            let mut query: Vec<(&str, String)> = Vec::new();
            if let ItemId::Inode(uuid) = &id {
                query.push(("id", uuid.to_string()));
            }
            let response = self.get_authed("item", &query).await?;
            if response.status() == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            Ok(Some(item_from_wire(parse_json::<MountItem>(response).await?)))
        })
    }

    fn enumerate(
        &self,
        parent: ItemId,
        cursor: Option<Cursor>,
    ) -> BoxFuture<'_, Result<Page, TransportError>> {
        Box::pin(async move {
            let mut query: Vec<(&str, String)> = Vec::new();
            if let ItemId::Inode(uuid) = &parent {
                query.push(("parent_id", uuid.to_string()));
            }
            if let Some(cursor) = &cursor {
                query.push(("cursor", cursor.0.clone()));
            }
            let response = self.get_authed("enumerate", &query).await?;
            let wire = parse_json::<MountEnumerateResponse>(response).await?;
            Ok(Page {
                items: wire.items.into_iter().map(item_from_wire).collect(),
                next: wire.next_cursor.map(Cursor),
            })
        })
    }

    fn health(&self) -> BoxFuture<'_, Result<Health, TransportError>> {
        Box::pin(async move {
            // Unauthenticated by design — probeable before any token exists.
            let response = self
                .client
                .get(self.url("health"))
                .send()
                .await
                .map_err(map_reqwest_err)?;
            let wire = parse_json::<HealthResponse>(response).await?;
            Ok(match wire.status {
                HealthStatus::Ready => Health::Ready,
                HealthStatus::NotReady => Health::NotReady,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hopnet_common::CustomUUID;

    fn wire_file(name: &str) -> MountItem {
        MountItem {
            id: Some(CustomUUID::new(None)),
            parent_id: None,
            name: name.to_string(),
            item_type: InodeType::File,
            size: Some(640),
            blob_id: Some(CustomUUID::new(None)),
            created_ms: 1_785_444_148_746,
            modified_ms: Some(1_785_444_148_589),
            height: Some(4),
        }
    }

    // Should: convert wire epoch-milliseconds into SystemTime exactly,
    // for both created and modified stamps.
    #[test]
    fn wire_times_map_to_system_time() {
        let item = item_from_wire(wire_file("t.txt"));
        assert_eq!(
            item.created,
            UNIX_EPOCH + Duration::from_millis(1_785_444_148_746)
        );
        assert_eq!(
            item.modified,
            UNIX_EPOCH + Duration::from_millis(1_785_444_148_589)
        );
    }

    // Should: map absent wire ids to the Root item id on both the item
    // itself and its parent linkage.
    #[test]
    fn absent_wire_ids_mean_root() {
        let mut wire = wire_file("t.txt");
        wire.id = None;
        let item = item_from_wire(wire);
        assert_eq!(item.id, ItemId::Root);
        assert_eq!(item.parent, ItemId::Root);
    }

    // Should: carry kind, size, blob id, and height through the mapping.
    // Should not: invent a blob for folders.
    #[test]
    fn kind_size_blob_and_height_survive() {
        let wire = wire_file("t.txt");
        let blob = wire.blob_id.clone();
        let item = item_from_wire(wire);
        assert_eq!(item.kind, ItemKind::File { size: 640 });
        assert_eq!(item.blob, blob);
        assert_eq!(item.height, 4);

        let folder = MountItem {
            id: Some(CustomUUID::new(None)),
            parent_id: None,
            name: "d".to_string(),
            item_type: InodeType::Folder,
            size: None,
            blob_id: None,
            created_ms: 0,
            modified_ms: None,
            height: None,
        };
        let folder = item_from_wire(folder);
        assert_eq!(folder.kind, ItemKind::Folder);
        assert_eq!(folder.blob, None);
        assert_eq!(folder.height, 0);
    }
}
