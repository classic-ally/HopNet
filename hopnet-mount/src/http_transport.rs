//! HTTP implementation of `NodeTransport` (RFC-018 S3) against the node's
//! `/api/integrations/mount` surface (S2).
//!
//! Auth is an RFC-012 device token (`Bearer {device_id}.{secret_hex}`) —
//! bootstrap sessions mean no prior login is needed. Failure discipline
//! per the RFC: one bounded retry on connection errors, then a loud
//! `Unavailable` — never an indefinite hang.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hopnet_common::CustomUUID;
use hopnet_common::db::InodeType;
use hopnet_common::fileprovider::{HealthResponse, HealthStatus};
use hopnet_common::mount::{
    MountChangesResponse, MountEnumerateResponse, MountItem, MountStatfsResponse,
};

use crate::transport::{
    BoxFuture, Changes, Cursor, Health, Height, Item, ItemId, ItemKind, Mutated, NodeTransport,
    Page, StatfsInfo, TransportError, WatchEvent, WatchStream,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

pub struct HttpTransport {
    client: reqwest::Client,
    /// Connect-timeout only — content uploads and consensus waits outlive
    /// any fixed request timeout (a 100 GB upload, a 120 s decide).
    upload_client: reqwest::Client,
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
        let upload_client = reqwest::Client::builder()
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| TransportError::Protocol(e.to_string()))?;
        Ok(HttpTransport {
            client,
            upload_client,
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

/// Shared mutation-response handling: strict-route status mapping, then
/// MountMutationResponse → Mutated.
async fn parse_mutation(response: reqwest::Response) -> Result<Mutated, TransportError> {
    let status = response.status();
    if status == reqwest::StatusCode::UNAUTHORIZED
        || status == reqwest::StatusCode::PRECONDITION_REQUIRED
    {
        return Err(TransportError::Unauthorized);
    }
    if status == reqwest::StatusCode::CONFLICT {
        return Err(TransportError::Conflict);
    }
    if status == reqwest::StatusCode::GATEWAY_TIMEOUT {
        return Err(TransportError::OutcomeUnknown);
    }
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(TransportError::Protocol("item gone".to_string()));
    }
    if !status.is_success() {
        return Err(TransportError::Protocol(format!(
            "unexpected status {status}"
        )));
    }
    let wire = response
        .json::<hopnet_common::mount::MountMutationResponse>()
        .await
        .map_err(|e| TransportError::Protocol(e.to_string()))?;
    Ok(Mutated {
        item: wire.item.map(item_from_wire),
        height: Height::from(wire.height),
    })
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

    fn changes(&self, since: Height) -> BoxFuture<'_, Result<Changes, TransportError>> {
        Box::pin(async move {
            // Wire heights are i32; clamp the anchor-init sentinel.
            let since_wire = since.clamp(0, i32::MAX as Height) as i32;
            let query = vec![("since_height", since_wire.to_string())];
            let response = self.get_authed("changes", &query).await?;
            let wire = parse_json::<MountChangesResponse>(response).await?;
            Ok(Changes {
                items: wire.items.into_iter().map(item_from_wire).collect(),
                deleted: wire.deleted_ids,
                height: Height::from(wire.height),
            })
        })
    }

    fn watch(&self) -> BoxFuture<'_, Result<WatchStream, TransportError>> {
        Box::pin(async move {
            // Separate client: the normal 30 s request timeout would kill
            // a long-lived SSE stream. Connect timeout still applies;
            // liveness is the watch loop's job (heartbeat-bounded).
            let client = reqwest::Client::builder()
                .connect_timeout(CONNECT_TIMEOUT)
                .build()
                .map_err(|e| TransportError::Protocol(e.to_string()))?;
            let response = client
                .get(self.url("watch"))
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(map_reqwest_err)?;
            let response = check_status(response)?;
            if !response.status().is_success() {
                return Err(TransportError::Protocol(format!(
                    "unexpected status {}",
                    response.status()
                )));
            }

            // Minimal SSE parse over the byte stream: `data:` lines are
            // pokes, `:` comment lines are heartbeats, everything else
            // (event/id fields, blank separators) is ignored.
            let mut bytes = response.bytes_stream();
            let stream = async_stream::stream! {
                let mut buffer: Vec<u8> = Vec::new();
                use tokio_stream::StreamExt;
                while let Some(chunk) = bytes.next().await {
                    let Ok(chunk) = chunk else { break };
                    buffer.extend_from_slice(&chunk);
                    while let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
                        let line: Vec<u8> = buffer.drain(..=pos).collect();
                        let line = String::from_utf8_lossy(&line);
                        let line = line.trim_end();
                        if line.starts_with("data:") {
                            yield WatchEvent::Poke;
                        } else if line.starts_with(':') {
                            yield WatchEvent::Heartbeat;
                        }
                    }
                }
            };
            Ok(Box::pin(stream) as WatchStream)
        })
    }

    fn read_blob(
        &self,
        blob: CustomUUID,
        offset: u64,
        len: u64,
    ) -> BoxFuture<'_, Result<Vec<u8>, TransportError>> {
        Box::pin(async move {
            if len == 0 {
                return Ok(Vec::new());
            }
            let query = vec![("blob_id", blob.to_string())];
            let range = format!("bytes={}-{}", offset, offset + len - 1);
            for attempt in 0..2 {
                let result = self
                    .client
                    .get(self.url("download"))
                    .bearer_auth(&self.token)
                    .header(reqwest::header::RANGE, &range)
                    .query(&query)
                    .send()
                    .await;
                let response = match result {
                    Ok(response) => response,
                    Err(e) if is_transport_level(&e) && attempt == 0 => continue,
                    Err(e) => return Err(map_reqwest_err(e)),
                };
                let response = check_status(response)?;
                let status = response.status();
                // 416 past EOF = empty (callers clamp, but a raced
                // shrink shouldn't error a read).
                if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                    return Ok(Vec::new());
                }
                if status == reqwest::StatusCode::NOT_FOUND {
                    // Blob collected under an open handle (issue #26).
                    return Err(TransportError::Protocol("blob gone".to_string()));
                }
                if !status.is_success() {
                    return Err(TransportError::Protocol(format!(
                        "unexpected status {status}"
                    )));
                }
                let body = response
                    .bytes()
                    .await
                    .map_err(map_reqwest_err)?;
                return Ok(body.to_vec());
            }
            unreachable!("loop returns on second attempt")
        })
    }

    fn create_folder(
        &self,
        parent: ItemId,
        name: String,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>> {
        Box::pin(async move {
            let mut form = reqwest::multipart::Form::new().text("folder_name", name);
            if let ItemId::Inode(uuid) = &parent {
                form = form.text("parent_id", uuid.to_string());
            }
            let response = self
                .upload_client
                .post(self.url("create"))
                .bearer_auth(&self.token)
                .multipart(form)
                .send()
                .await
                .map_err(map_reqwest_err)?;
            parse_mutation(response).await
        })
    }

    fn create_file(
        &self,
        parent: ItemId,
        name: String,
        size: u64,
        content: crate::transport::ByteSource,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>> {
        Box::pin(async move {
            let part = reqwest::multipart::Part::stream_with_length(
                reqwest::Body::wrap_stream(content),
                size,
            )
            .file_name(name);
            let mut form = reqwest::multipart::Form::new();
            if let ItemId::Inode(uuid) = &parent {
                form = form.text("parent_id", uuid.to_string());
            }
            let form = form.part(format!("file_{size}"), part);
            let response = self
                .upload_client
                .post(self.url("create"))
                .bearer_auth(&self.token)
                .multipart(form)
                .send()
                .await
                .map_err(map_reqwest_err)?;
            parse_mutation(response).await
        })
    }

    fn update_content(
        &self,
        id: CustomUUID,
        size: u64,
        content: crate::transport::ByteSource,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>> {
        Box::pin(async move {
            let part = reqwest::multipart::Part::stream_with_length(
                reqwest::Body::wrap_stream(content),
                size,
            )
            .file_name("content");
            let form = reqwest::multipart::Form::new()
                .text("inode_id", id.to_string())
                .part(format!("file_{size}"), part);
            let response = self
                .upload_client
                .put(self.url("content"))
                .bearer_auth(&self.token)
                .multipart(form)
                .send()
                .await
                .map_err(map_reqwest_err)?;
            parse_mutation(response).await
        })
    }

    fn rename(
        &self,
        id: CustomUUID,
        new_parent: Option<ItemId>,
        new_name: Option<String>,
    ) -> BoxFuture<'_, Result<Mutated, TransportError>> {
        Box::pin(async move {
            let (new_parent_id, new_parent_root) = match new_parent {
                Some(ItemId::Inode(uuid)) => (Some(uuid), false),
                Some(ItemId::Root) => (None, true),
                None => (None, false),
            };
            let body = serde_json::json!({
                "id": id,
                "new_parent_id": new_parent_id,
                "new_parent_root": new_parent_root,
                "new_name": new_name,
            });
            // Mutations wait on consensus (bounded by the node's 120 s);
            // use the upload client so 30 s doesn't cut the wait short.
            let response = self
                .upload_client
                .patch(self.url("modify"))
                .bearer_auth(&self.token)
                .json(&body)
                .send()
                .await
                .map_err(map_reqwest_err)?;
            parse_mutation(response).await
        })
    }

    fn delete(
        &self,
        id: CustomUUID,
        recursive: bool,
    ) -> BoxFuture<'_, Result<Height, TransportError>> {
        Box::pin(async move {
            let response = self
                .upload_client
                .delete(self.url("delete"))
                .bearer_auth(&self.token)
                .json(&serde_json::json!({ "id": id, "recursive": recursive }))
                .send()
                .await
                .map_err(map_reqwest_err)?;
            parse_mutation(response).await.map(|m| m.height)
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

    fn statfs(&self) -> BoxFuture<'_, Result<StatfsInfo, TransportError>> {
        Box::pin(async move {
            let response = self
                .client
                .get(self.url("statfs"))
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(map_reqwest_err)?;
            let wire = parse_json::<MountStatfsResponse>(response).await?;
            Ok(StatfsInfo {
                total_bytes: wire.total_bytes,
                used_bytes: wire.used_bytes,
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
