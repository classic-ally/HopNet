//! HopNet photo publisher: turns a `PhotoAsset` into encrypted resources and
//! submits a `photo_add` consensus transaction (RFC-011 content pipeline).
//!
//! The publisher is a free async function, not a trait.
//!
//! ## Idempotency contract
//!
//! The caller must persist `(SourceIdentity -> photo_id)` before publishing.
//!
//! After any ambiguous submit failure (timeout, lost acknowledgement), the
//! caller MUST query committed photo state for that `photo_id`. Uploaded blob
//! IDs in `PartialPublish` are reconciliation candidates, not known orphans
//! — they are safe to GC only after absence of the photo row is confirmed.
//!
//! Retry with the SAME `photo_id` is safe only after confirming no photo row
//! exists. If the photo row exists, the original publish succeeded and the
//! caller must record success instead of retrying or scheduling blobs for GC.
//!
//! Stream length is enforced inline during upload, so a bad-length stream
//! fails MID-put: for multi-chunk (>40 MB) resources this can leave orphaned
//! fragments on disk until the substrate's orphaned-fragment maintenance
//! sweep collects them (same behavior as the drive upload path; single-chunk
//! puts fail before any fragment write).

use std::collections::HashSet;

use hopnet_common::CustomUUID;
use hopnet_photos::envelopes::{MetadataAccessEntry, PhotoResourceOp};
use hopnet_storage::store::BlobInsertOp;

use crate::asset::{PhotoAsset, ResourceKind};
use crate::crypto::{
    encrypt_metadata, generate_blob_key, generate_metadata_key, wrap_blob_key_for_recipients,
    wrap_metadata_key,
};
use crate::dispatch::{LibraryMembership, PhotoDispatch, UploadedFragment};
use crate::error::{PhotosCoreError, PublishValidationError};
use crate::metadata::PhotoMetadata;
use crate::payloads::{
    PhotoAddDraft, PhotoEditContentDraft, PhotoEditMetadataDraft, build_photo_add,
    build_photo_edit_content, build_photo_edit_metadata, encode_payload,
};

pub enum ByteSource {
    Stream(Box<dyn tokio::io::AsyncRead + Unpin + Send>),
}

impl std::fmt::Debug for ByteSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream(_) => f.debug_tuple("Stream").field(&"<reader>").finish(),
        }
    }
}

pub struct PublishRequest<'a> {
    pub asset: &'a PhotoAsset,
    pub photo_id: CustomUUID,
    pub library_id: Option<CustomUUID>,
    pub byte_sources: Vec<(ResourceKind, ByteSource)>,
    /// Keyed HMAC of the asset's stable cross-device id, obtained from the
    /// node's resolve route pre-publish. None = local-only asset (no dedupe).
    pub cloud_fingerprint: Option<[u8; 32]>,
}

pub struct IngestOutcome {
    pub photo_id: CustomUUID,
    pub operation_id: CustomUUID,
    pub resources: Vec<(ResourceKind, hopnet_storage::BlobId)>,
}

impl std::fmt::Debug for IngestOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestOutcome")
            .field("photo_id", &self.photo_id)
            .field("operation_id", &self.operation_id)
            .finish_non_exhaustive()
    }
}

fn fragment_to_meta(
    blob_id: hopnet_storage::BlobId,
    f: &UploadedFragment,
) -> hopnet_storage::store::FragmentMeta {
    hopnet_storage::store::FragmentMeta {
        blob_id,
        chunk_number: f.chunk_number,
        local_index: f.local_index,
        fragment_id: f.fragment_id.clone(),
        fragment_hash: f.fragment_hash,
        recovery: f.recovery,
    }
}

fn validate_request(
    asset: &PhotoAsset,
    byte_sources: &[(ResourceKind, ByteSource)],
    membership: &LibraryMembership,
    library_id: Option<&CustomUUID>,
) -> Result<(), PublishValidationError> {
    let mut declared = 0u16;
    for resource in &asset.resources {
        declared |= 1u16 << resource.kind.as_wire();
    }

    let mut supplied = 0u16;
    for (kind, _) in byte_sources {
        let bit = 1u16 << kind.as_wire();
        if supplied & bit != 0 {
            return Err(PublishValidationError::DuplicateByteSource(*kind));
        }
        supplied |= bit;

        if declared & bit == 0 {
            return Err(PublishValidationError::UnexpectedByteSource(*kind));
        }
    }

    for resource in &asset.resources {
        let bit = 1u16 << resource.kind.as_wire();
        if supplied & bit == 0 {
            return Err(PublishValidationError::MissingByteSource(resource.kind));
        }
        let content = resource;
        let _ = usize::try_from(content.content.byte_len).map_err(|_| {
            PublishValidationError::ResourceTooLarge {
                kind: resource.kind,
                byte_len: content.content.byte_len,
            }
        })?;
        let _ = i64::try_from(content.content.byte_len).map_err(|_| {
            PublishValidationError::ResourceTooLarge {
                kind: resource.kind,
                byte_len: content.content.byte_len,
            }
        })?;
    }

    validate_recipients(membership, library_id)
}

/// The recipient half of publish validation, shared with the edit path —
/// every key wrap an edit mints is aimed at this same member set, so a
/// membership the add path would refuse must not be usable for an edit.
fn validate_recipients(
    membership: &LibraryMembership,
    library_id: Option<&CustomUUID>,
) -> Result<(), PublishValidationError> {
    let uploaded_by = membership.uploaded_by;
    let members = &membership.members;

    if members.is_empty() {
        return Err(PublishValidationError::NoRecipients);
    }

    let mut user_ids = HashSet::with_capacity(members.len());
    for member in members {
        if !user_ids.insert(member.user_id) {
            return Err(PublishValidationError::DuplicateRecipient(member.user_id));
        }
    }

    if library_id.is_none() && (members.len() != 1 || members[0].user_id != uploaded_by) {
        return Err(PublishValidationError::InvalidPersonalRecipients {
            uploaded_by,
            member_ids: members.iter().map(|m| m.user_id).collect(),
        });
    }

    if library_id.is_some() && !members.iter().any(|m| m.user_id == uploaded_by) {
        return Err(PublishValidationError::UploaderNotMember { uploaded_by });
    }

    Ok(())
}

/// Typed payload carried inside the io::Error that `ExactLen` raises, so the
/// publisher can recover the validation error after it round-trips through
/// the dispatch's storage put as `StorageError::Read`.
#[derive(Debug, Clone)]
struct ExactLenError {
    kind: ResourceKind,
    expected: u64,
    /// `Some(actual)` = stream too short; `None` = stream too long.
    actual: Option<u64>,
}

impl std::fmt::Display for ExactLenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.actual {
            Some(actual) => write!(
                f,
                "resource {} stream ended at {actual} of {} bytes",
                self.kind, self.expected
            ),
            None => write!(
                f,
                "resource {} stream exceeds declared {} bytes",
                self.kind, self.expected
            ),
        }
    }
}

impl std::error::Error for ExactLenError {}

/// Enforces that the inner reader yields exactly `expected` bytes: premature
/// EOF and overlong streams both surface as io::Errors carrying a typed
/// `ExactLenError`, recovered by `map_exact_len_error` after the upload.
struct ExactLen<R> {
    inner: R,
    kind: ResourceKind,
    expected: u64,
    consumed: u64,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for ExactLen<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();

        if this.consumed == this.expected {
            // Probe one extra read: a clean EOF passes, any byte is TooLong.
            let mut probe_storage = [0u8; 1];
            let mut probe = tokio::io::ReadBuf::new(&mut probe_storage);
            std::task::ready!(std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut probe))?;
            return std::task::Poll::Ready(if probe.filled().is_empty() {
                Ok(())
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    ExactLenError {
                        kind: this.kind,
                        expected: this.expected,
                        actual: None,
                    },
                ))
            });
        }

        let remaining = usize::try_from(this.expected - this.consumed)
            .unwrap_or(usize::MAX)
            .min(buf.remaining());
        let mut sub = buf.take(remaining);
        std::task::ready!(std::pin::Pin::new(&mut this.inner).poll_read(cx, &mut sub))?;
        let n = sub.filled().len();
        if n == 0 {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                ExactLenError {
                    kind: this.kind,
                    expected: this.expected,
                    actual: Some(this.consumed),
                },
            )));
        }
        // SAFETY: `sub` borrows `buf`'s unfilled region, so its first `n`
        // bytes are now initialized (standard tokio `Take` pattern).
        unsafe { buf.assume_init(n) };
        buf.advance(n);
        this.consumed += n as u64;
        std::task::Poll::Ready(Ok(()))
    }
}

/// Recovers the typed length-validation error from an upload failure whose
/// io::Error originated in `ExactLen`; passes every other error through.
fn map_exact_len_error(err: PhotosCoreError) -> PhotosCoreError {
    if let PhotosCoreError::Storage(hopnet_storage::StorageError::Read(ref io_err)) = err
        && let Some(e) = io_err
            .get_ref()
            .and_then(|b| b.downcast_ref::<ExactLenError>())
    {
        return match e.actual {
            Some(actual) => PublishValidationError::ResourceTooShort {
                kind: e.kind,
                expected: e.expected,
                actual,
            }
            .into(),
            None => PublishValidationError::ResourceTooLong {
                kind: e.kind,
                expected: e.expected,
            }
            .into(),
        };
    }
    err
}

fn partial_publish(
    photo_id: CustomUUID,
    blob_ids: &[(ResourceKind, hopnet_storage::BlobId)],
    source: PhotosCoreError,
) -> PhotosCoreError {
    PhotosCoreError::PartialPublish {
        photo_id,
        uploaded_blob_ids: blob_ids.iter().map(|(_, id)| id.clone()).collect(),
        source: Box::new(source),
    }
}

/// Upload one resource's bytes and mint its wire op: fresh blob id, fresh
/// blob key, length-enforced stream, key wrapped to every recipient.
///
/// `uploaded` gains the blob id as soon as the bytes reach the substrate —
/// BEFORE the key wrap — so a wrap failure still reports what landed. The
/// caller decides whether that makes the failure a partial publish.
async fn upload_resource(
    dispatch: &dyn PhotoDispatch,
    kind: ResourceKind,
    byte_len: u64,
    source: ByteSource,
    recipients: &[hopnet_storage::x25519_dalek::PublicKey],
    uploaded: &mut Vec<(ResourceKind, hopnet_storage::BlobId)>,
) -> Result<PhotoResourceOp, PhotosCoreError> {
    let ByteSource::Stream(reader) = source;
    let upload_len = byte_len as usize;
    let wrapped = Box::new(ExactLen {
        inner: reader,
        kind,
        expected: byte_len,
        consumed: 0,
    });

    let blob_id = CustomUUID::new(None);
    let blob_key = generate_blob_key();

    let outcome = dispatch
        .upload_data_block(blob_id.clone(), wrapped, upload_len, blob_key)
        .await
        .map_err(map_exact_len_error)?;
    uploaded.push((kind, blob_id.clone()));

    let access = wrap_blob_key_for_recipients(&blob_id, recipients, &blob_key)?;
    let fragments: Vec<hopnet_storage::store::FragmentMeta> = outcome
        .fragments
        .iter()
        .map(|f| fragment_to_meta(blob_id.clone(), f))
        .collect();

    Ok(PhotoResourceOp {
        resource_type: kind.as_wire(),
        op: BlobInsertOp {
            blob_id,
            integrity_hash: outcome.integrity_hash,
            added_bytes: outcome.added_bytes,
            file_size: byte_len,
            fragments,
            access,
        },
    })
}

pub async fn publish_photo_add(
    dispatch: &dyn PhotoDispatch,
    mut req: PublishRequest<'_>,
) -> Result<IngestOutcome, PhotosCoreError> {
    req.asset.validate()?;

    let photo_id = req.photo_id.clone();
    let library_id = req.library_id.clone();

    let membership = dispatch.fetch_library_members(library_id.clone()).await?;
    let uploaded_by = membership.uploaded_by;
    let recipient_pubkeys: Vec<_> = membership.members.iter().map(|m| m.pubkey).collect();

    validate_request(
        req.asset,
        &req.byte_sources,
        &membership,
        library_id.as_ref(),
    )?;

    let metadata_key = generate_metadata_key();
    let (encrypted_metadata, metadata_nonce) =
        encrypt_metadata(&metadata_key, &req.asset.metadata.to_json()?)?;

    let mut resource_ops: Vec<PhotoResourceOp> = Vec::with_capacity(req.byte_sources.len());
    let mut blob_ids: Vec<(ResourceKind, hopnet_storage::BlobId)> =
        Vec::with_capacity(req.byte_sources.len());

    for i in 0..req.byte_sources.len() {
        let kind = req.byte_sources[i].0;
        let source = std::mem::replace(
            &mut req.byte_sources[i].1,
            ByteSource::Stream(Box::new(tokio::io::empty())),
        );
        let byte_len = req
            .asset
            .resource(kind)
            .expect("byte source kinds validated against asset resources")
            .content
            .byte_len;

        let op = match upload_resource(
            dispatch,
            kind,
            byte_len,
            source,
            &recipient_pubkeys,
            &mut blob_ids,
        )
        .await
        {
            Ok(op) => op,
            Err(error) if blob_ids.is_empty() => return Err(error),
            Err(error) => return Err(partial_publish(photo_id, &blob_ids, error)),
        };
        resource_ops.push(op);
    }

    let metadata_access: Vec<MetadataAccessEntry> = membership
        .members
        .iter()
        .map(|m| {
            let (eph, wrapped) = wrap_metadata_key(&photo_id, &m.pubkey, &metadata_key)?;
            Ok(MetadataAccessEntry {
                user_id: m.user_id,
                ephemeral_pubkey: eph,
                encrypted_metadata_key: wrapped,
            })
        })
        .collect::<Result<_, PhotosCoreError>>()?;

    let operation_id = CustomUUID::new(None);
    let draft = PhotoAddDraft {
        photo_id: photo_id.clone(),
        library_id,
        uploaded_by,
        encrypted_metadata,
        metadata_nonce,
        resources: resource_ops,
        metadata_access,
        operation_id: operation_id.clone(),
        cloud_fingerprint: req.cloud_fingerprint,
    };
    let payload = match build_photo_add_and_encode(draft) {
        Ok(p) => p,
        Err(e) if blob_ids.is_empty() => return Err(e),
        Err(e) => return Err(partial_publish(photo_id, &blob_ids, e)),
    };

    if let Err(e) = dispatch.submit_transaction("photo_add", payload).await {
        return Err(partial_publish(photo_id, &blob_ids, e));
    }

    Ok(IngestOutcome {
        photo_id,
        operation_id,
        resources: blob_ids,
    })
}

fn build_photo_add_and_encode(draft: PhotoAddDraft) -> Result<Vec<u8>, PhotosCoreError> {
    let payload = build_photo_add(vec![draft]);
    encode_payload(&payload)
}

/// One resource whose bytes an edit replaces.
pub struct EditResource {
    pub kind: ResourceKind,
    pub byte_len: u64,
    pub source: ByteSource,
}

impl std::fmt::Debug for EditResource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EditResource")
            .field("kind", &self.kind)
            .field("byte_len", &self.byte_len)
            .finish_non_exhaustive()
    }
}

/// A re-edit, revert, or metadata refresh of an ALREADY published photo.
///
/// Unlike [`PublishRequest`] this carries no [`PhotoAsset`]: an edit names
/// only what changed, and a photo's `Original` is exactly what an edit never
/// touches — so the asset's own validation, which requires one, does not
/// apply here.
pub struct EditRequest<'a> {
    pub photo_id: CustomUUID,
    pub library_id: Option<CustomUUID>,
    /// Resources whose bytes changed. The first becomes the primary in the
    /// operation log (prior → new); the rest ride alongside it.
    pub resources: Vec<EditResource>,
    /// Kinds a revert dropped. The mesh has no way to express an absence
    /// through an upsert, so removals travel as their own list.
    pub remove_resources: Vec<ResourceKind>,
    /// New metadata, when it changed with the pixels (a crop's dimensions)
    /// or on its own. Always re-encrypted under a FRESH key and re-wrapped:
    /// the publisher holds no member private key, so it can never recover
    /// the key the photo was published under.
    pub metadata: Option<&'a PhotoMetadata>,
}

fn validate_edit(
    req: &EditRequest<'_>,
    membership: &LibraryMembership,
    library_id: Option<&CustomUUID>,
) -> Result<(), PublishValidationError> {
    if req.resources.is_empty() && req.remove_resources.is_empty() && req.metadata.is_none() {
        return Err(PublishValidationError::EmptyEdit);
    }

    let mut supplied = 0u16;
    for resource in &req.resources {
        let bit = 1u16 << resource.kind.as_wire();
        if supplied & bit != 0 {
            return Err(PublishValidationError::DuplicateByteSource(resource.kind));
        }
        supplied |= bit;

        let _ = usize::try_from(resource.byte_len).map_err(|_| {
            PublishValidationError::ResourceTooLarge {
                kind: resource.kind,
                byte_len: resource.byte_len,
            }
        })?;
        let _ = i64::try_from(resource.byte_len).map_err(|_| {
            PublishValidationError::ResourceTooLarge {
                kind: resource.kind,
                byte_len: resource.byte_len,
            }
        })?;
    }

    let mut removed = 0u16;
    for kind in &req.remove_resources {
        let bit = 1u16 << kind.as_wire();
        if removed & bit != 0 {
            return Err(PublishValidationError::DuplicateRemoval(*kind));
        }
        if supplied & bit != 0 {
            return Err(PublishValidationError::EditedAndRemoved(*kind));
        }
        removed |= bit;
    }

    validate_recipients(membership, library_id)
}

/// Tell the mesh what Photos did to an already-published photo: new bytes
/// for edited resources, removals for a revert, and refreshed metadata.
///
/// The photo id is the one consensus already holds (for an adopted photo,
/// the first publisher's id — not the local one). Both transactions are
/// idempotent replacements, so a retry after an ambiguous submit is safe
/// without a confirm probe; unlike `photo_add` there is no unique id to
/// collide with.
pub async fn publish_photo_edit(
    dispatch: &dyn PhotoDispatch,
    req: EditRequest<'_>,
) -> Result<IngestOutcome, PhotosCoreError> {
    let photo_id = req.photo_id.clone();
    let library_id = req.library_id.clone();

    let membership = dispatch.fetch_library_members(library_id.clone()).await?;
    validate_edit(&req, &membership, library_id.as_ref())?;
    let recipient_pubkeys: Vec<_> = membership.members.iter().map(|m| m.pubkey).collect();

    // Fresh key + fresh wraps, or neither. Replacing the ciphertext while
    // leaving the stored wraps pointing at the old key would make the
    // metadata undecryptable for every member, silently.
    let metadata = match req.metadata {
        Some(metadata) => {
            let key = generate_metadata_key();
            let (ciphertext, nonce) = encrypt_metadata(&key, &metadata.to_json()?)?;
            let access: Vec<MetadataAccessEntry> = membership
                .members
                .iter()
                .map(|m| {
                    let (eph, wrapped) = wrap_metadata_key(&photo_id, &m.pubkey, &key)?;
                    Ok(MetadataAccessEntry {
                        user_id: m.user_id,
                        ephemeral_pubkey: eph,
                        encrypted_metadata_key: wrapped,
                    })
                })
                .collect::<Result<_, PhotosCoreError>>()?;
            Some((ciphertext, nonce, access))
        }
        None => None,
    };

    let mut blob_ids: Vec<(ResourceKind, hopnet_storage::BlobId)> =
        Vec::with_capacity(req.resources.len());
    let mut resource_ops: Vec<PhotoResourceOp> = Vec::with_capacity(req.resources.len());
    for resource in req.resources {
        let EditResource {
            kind,
            byte_len,
            source,
        } = resource;
        let op = match upload_resource(
            dispatch,
            kind,
            byte_len,
            source,
            &recipient_pubkeys,
            &mut blob_ids,
        )
        .await
        {
            Ok(op) => op,
            Err(error) if blob_ids.is_empty() => return Err(error),
            Err(error) => return Err(partial_publish(photo_id, &blob_ids, error)),
        };
        resource_ops.push(op);
    }

    let operation_id = CustomUUID::new(None);
    let touches_content = !resource_ops.is_empty() || !req.remove_resources.is_empty();
    let (function, encoded) = if touches_content {
        let (encrypted_metadata, metadata_nonce, metadata_access) = match metadata {
            Some((ciphertext, nonce, access)) => (Some(ciphertext), Some(nonce), access),
            None => (None, None, Vec::new()),
        };
        let payload = build_photo_edit_content(vec![PhotoEditContentDraft {
            photo_id: photo_id.clone(),
            resources: resource_ops,
            encrypted_metadata,
            metadata_nonce,
            metadata_access,
            remove_resources: req
                .remove_resources
                .iter()
                .map(|kind| kind.as_wire())
                .collect(),
            operation_id: operation_id.clone(),
        }]);
        ("photo_edit_content", encode_payload(&payload))
    } else {
        let (encrypted_metadata, metadata_nonce, metadata_access) =
            metadata.expect("validate_edit rejects an edit that changes nothing");
        let payload = build_photo_edit_metadata(vec![PhotoEditMetadataDraft {
            photo_id: photo_id.clone(),
            encrypted_metadata,
            metadata_nonce,
            metadata_access,
            operation_id: operation_id.clone(),
        }]);
        ("photo_edit_metadata", encode_payload(&payload))
    };
    let encoded = match encoded {
        Ok(bytes) => bytes,
        Err(e) if blob_ids.is_empty() => return Err(e),
        Err(e) => return Err(partial_publish(photo_id, &blob_ids, e)),
    };

    if let Err(e) = dispatch.submit_transaction(function, encoded).await {
        return Err(partial_publish(photo_id, &blob_ids, e));
    }

    Ok(IngestOutcome {
        photo_id,
        operation_id,
        resources: blob_ids,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asset::{PhotoAsset, PhotoResource, ResourceContent, SourceIdentity};
    use crate::crypto::{decrypt_metadata, unwrap_metadata_key};
    use crate::dispatch::{
        LibraryMember, LibraryMembership, PhotoDispatch, SyncBatch, UploadedDataBlock,
        UploadedFragment,
    };
    use crate::metadata::PhotoMetadata;
    use hopnet_storage::StaticRecipient;
    use std::sync::Mutex;

    struct CallLog {
        uploads: Vec<hopnet_storage::BlobId>,
        submits: Vec<(String, Vec<u8>)>,
        library_fetches: Vec<Option<CustomUUID>>,
    }

    struct MockDispatch {
        log: Mutex<CallLog>,
        fragments_dir: tempfile::TempDir,
        upload_fail_after: Option<usize>,
        submit_fail: bool,
        uploaded_by: i32,
        members: Vec<LibraryMember>,
    }

    fn fixed_members() -> Vec<LibraryMember> {
        let secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xAB; 32]);
        let pubkey = hopnet_storage::x25519_dalek::PublicKey::from(&secret);
        vec![LibraryMember { user_id: 1, pubkey }]
    }

    impl MockDispatch {
        fn new(fragments_dir: tempfile::TempDir) -> Self {
            Self {
                log: Mutex::new(CallLog {
                    uploads: Vec::new(),
                    submits: Vec::new(),
                    library_fetches: Vec::new(),
                }),
                fragments_dir,
                upload_fail_after: None,
                submit_fail: false,
                uploaded_by: 1,
                members: fixed_members(),
            }
        }

        fn with_submit_fail(mut self) -> Self {
            self.submit_fail = true;
            self
        }

        fn with_upload_fail_after(mut self, n: usize) -> Self {
            self.upload_fail_after = Some(n);
            self
        }

        fn with_members(mut self, members: Vec<LibraryMember>) -> Self {
            self.members = members;
            self
        }
    }

    #[async_trait::async_trait]
    impl PhotoDispatch for MockDispatch {
        async fn submit_transaction(
            &self,
            tx_type: &str,
            payload_bytes: Vec<u8>,
        ) -> Result<(), PhotosCoreError> {
            self.log
                .lock()
                .unwrap()
                .submits
                .push((tx_type.to_string(), payload_bytes));
            if self.submit_fail {
                return Err(PhotosCoreError::Dispatch("mock submit failure".into()));
            }
            Ok(())
        }

        async fn fetch_photos_since(&self, _height: u64) -> Result<SyncBatch, PhotosCoreError> {
            unimplemented!()
        }

        async fn upload_data_block(
            &self,
            blob_id: hopnet_storage::BlobId,
            source: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
            file_size: usize,
            per_blob_key: chacha20poly1305::Key,
        ) -> Result<UploadedDataBlock, PhotosCoreError> {
            let idx = self.log.lock().unwrap().uploads.len();
            if let Some(n) = self.upload_fail_after
                && idx >= n
            {
                return Err(PhotosCoreError::Storage(hopnet_storage::StorageError::Rs));
            }
            let outcome = hopnet_storage::api::put(
                source,
                file_size,
                blob_id.clone(),
                &per_blob_key,
                self.fragments_dir.path().to_str().unwrap(),
            )
            .await?;
            self.log.lock().unwrap().uploads.push(blob_id);
            Ok(UploadedDataBlock {
                integrity_hash: outcome.integrity_hash,
                fragments: outcome
                    .fragments
                    .iter()
                    .map(|f| UploadedFragment {
                        chunk_number: f.chunk_number,
                        local_index: f.local_index,
                        fragment_id: f.fragment_id.clone(),
                        fragment_hash: f.fragment_hash,
                        recovery: f.recovery,
                    })
                    .collect(),
                added_bytes: outcome.added_bytes,
            })
        }

        async fn fetch_library_members(
            &self,
            library_id: Option<CustomUUID>,
        ) -> Result<LibraryMembership, PhotosCoreError> {
            self.log.lock().unwrap().library_fetches.push(library_id);
            Ok(LibraryMembership {
                uploaded_by: self.uploaded_by,
                members: self.members.clone(),
            })
        }
    }

    fn test_asset() -> PhotoAsset {
        asset_with_resources(&[ResourceKind::Original])
    }

    fn two_resource_asset() -> PhotoAsset {
        asset_with_resources(&[ResourceKind::Original, ResourceKind::Edited])
    }

    fn asset_with_resources(kinds: &[ResourceKind]) -> PhotoAsset {
        PhotoAsset {
            source: SourceIdentity::new("upload", "test-1"),
            metadata: PhotoMetadata {
                date_taken: "2025-01-01T00:00:00Z".into(),
                media_type: 0,
                width: Some(640),
                height: Some(480),
                ..Default::default()
            },
            resources: kinds
                .iter()
                .map(|k| PhotoResource {
                    kind: *k,
                    content: ResourceContent {
                        byte_len: 100_000,
                        content_hash: None,
                        format_hint: Some("image/jpeg".into()),
                    },
                })
                .collect(),
        }
    }

    fn test_stream(data: &'static [u8]) -> ByteSource {
        ByteSource::Stream(Box::new(std::io::Cursor::new(data)))
    }

    fn publish_req<'a>(
        asset: &'a PhotoAsset,
        byte_sources: Vec<(ResourceKind, ByteSource)>,
    ) -> PublishRequest<'a> {
        PublishRequest {
            asset,
            photo_id: CustomUUID::new(None),
            library_id: None,
            byte_sources,
            cloud_fingerprint: None,
        }
    }

    // --- validation ---

    #[tokio::test]
    async fn validates_asset_before_any_io() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let mut asset = test_asset();
        asset.resources.clear();
        let req = publish_req(&asset, vec![]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(err, PhotosCoreError::InvalidAsset(_)));
        assert_eq!(dispatch.log.lock().unwrap().uploads.len(), 0);
        assert_eq!(dispatch.log.lock().unwrap().submits.len(), 0);
    }

    #[tokio::test]
    async fn rejects_missing_byte_source() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let req = publish_req(&asset, vec![]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::MissingByteSource(
                ResourceKind::Original
            ))
        ));
    }

    #[tokio::test]
    async fn rejects_unexpected_byte_source() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let req = publish_req(
            &asset,
            vec![
                (ResourceKind::Original, test_stream(&[0x42; 100_000])),
                (ResourceKind::RawAlternate, test_stream(&[0x42; 100_000])),
            ],
        );
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::UnexpectedByteSource(
                ResourceKind::RawAlternate
            ))
        ));
    }

    #[tokio::test]
    async fn rejects_duplicate_byte_source() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let mut asset = test_asset();
        asset.resources.push(PhotoResource {
            kind: ResourceKind::Edited,
            content: ResourceContent {
                byte_len: 100_000,
                content_hash: None,
                format_hint: Some("image/jpeg".into()),
            },
        });
        let req = publish_req(
            &asset,
            vec![
                (ResourceKind::Original, test_stream(&[0x42; 100_000])),
                (ResourceKind::Original, test_stream(&[0x42; 100_000])),
            ],
        );
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::DuplicateByteSource(
                ResourceKind::Original
            ))
        ));
    }

    // --- staging ---

    #[tokio::test]
    async fn rejects_short_stream() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 100];
        let req = publish_req(&asset, vec![(ResourceKind::Original, test_stream(data))]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::ResourceTooShort { .. })
        ));
    }

    #[tokio::test]
    async fn rejects_long_stream() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 200_000];
        let req = publish_req(&asset, vec![(ResourceKind::Original, test_stream(data))]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::ResourceTooLong { .. })
        ));
    }

    // --- recipient validation ---

    #[tokio::test]
    async fn rejects_empty_recipients() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir).with_members(vec![]);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = publish_req(&asset, vec![(ResourceKind::Original, test_stream(data))]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::NoRecipients)
        ));
    }

    #[tokio::test]
    async fn rejects_personal_recipient_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let other_member = LibraryMember {
            user_id: 99,
            pubkey: fixed_members()[0].pubkey,
        };
        let dispatch = MockDispatch::new(dir).with_members(vec![other_member]);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = publish_req(&asset, vec![(ResourceKind::Original, test_stream(data))]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(
                PublishValidationError::InvalidPersonalRecipients { .. }
            )
        ));
    }

    // --- publish flow ---

    #[tokio::test]
    async fn uploads_every_resource_before_submit() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = two_resource_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = publish_req(
            &asset,
            vec![
                (ResourceKind::Original, test_stream(data)),
                (ResourceKind::Edited, test_stream(data)),
            ],
        );
        publish_photo_add(&dispatch, req).await.unwrap();
        let log = dispatch.log.lock().unwrap();
        assert_eq!(log.uploads.len(), 2);
        assert_eq!(log.submits.len(), 1);
    }

    #[tokio::test]
    async fn builds_decodable_photo_add_payload() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let photo_id = CustomUUID::new(None);
        let data: &'static [u8] = &[0x42; 100_000];
        let req = PublishRequest {
            asset: &asset,
            photo_id: photo_id.clone(),
            library_id: None,
            byte_sources: vec![(ResourceKind::Original, test_stream(data))],
            cloud_fingerprint: None,
        };
        let outcome = publish_photo_add(&dispatch, req).await.unwrap();
        assert_eq!(outcome.photo_id, photo_id);
        assert_eq!(outcome.resources.len(), 1);
        assert_eq!(outcome.resources[0].0, ResourceKind::Original);

        let log = dispatch.log.lock().unwrap();
        let (tx_type, payload_bytes) = &log.submits[0];
        assert_eq!(tx_type, "photo_add");
        let (decoded, _): (hopnet_photos::envelopes::PhotoAddPayload, _) =
            bincode::serde::decode_from_slice(payload_bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.entries.len(), 1);
        let entry = &decoded.entries[0];
        assert_eq!(entry.photo_id, photo_id);
        assert_eq!(entry.uploaded_by, 1);
        assert!(entry.library_id.is_none());
        assert_eq!(entry.resources.len(), 1);
        assert_eq!(entry.resources[0].resource_type, 0);
        assert_eq!(entry.metadata_access.len(), 1);
    }

    #[tokio::test]
    async fn encrypted_metadata_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = PublishRequest {
            asset: &asset,
            photo_id: CustomUUID::new(None),
            library_id: None,
            byte_sources: vec![(ResourceKind::Original, test_stream(data))],
            cloud_fingerprint: None,
        };
        publish_photo_add(&dispatch, req).await.unwrap();

        let log = dispatch.log.lock().unwrap();
        let (_, payload_bytes) = &log.submits[0];
        let (decoded, _): (hopnet_photos::envelopes::PhotoAddPayload, _) =
            bincode::serde::decode_from_slice(payload_bytes, bincode::config::standard()).unwrap();
        let entry = &decoded.entries[0];

        let secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xAB; 32]);
        let recipient = StaticRecipient(secret);
        let key = unwrap_metadata_key(
            &entry.photo_id,
            &entry.metadata_access[0].ephemeral_pubkey,
            &entry.metadata_access[0].encrypted_metadata_key,
            &recipient,
        )
        .unwrap();
        let decrypted =
            decrypt_metadata(&key, &entry.metadata_nonce, &entry.encrypted_metadata).unwrap();
        let round = PhotoMetadata::from_json(&decrypted).unwrap();
        assert_eq!(round.date_taken, "2025-01-01T00:00:00Z");
        assert_eq!(round.width, Some(640));
        assert_eq!(round.height, Some(480));
    }

    // --- edits ---

    fn edit_metadata() -> PhotoMetadata {
        PhotoMetadata {
            date_taken: "2025-01-01T00:00:00Z".into(),
            media_type: 0,
            width: Some(480),
            height: Some(480),
            ..Default::default()
        }
    }

    fn edit_req<'a>(
        resources: Vec<EditResource>,
        remove_resources: Vec<ResourceKind>,
        metadata: Option<&'a PhotoMetadata>,
    ) -> EditRequest<'a> {
        EditRequest {
            photo_id: CustomUUID::new(None),
            library_id: None,
            resources,
            remove_resources,
            metadata,
        }
    }

    fn edited_resource(kind: ResourceKind, data: &'static [u8]) -> EditResource {
        EditResource {
            kind,
            byte_len: data.len() as u64,
            source: test_stream(data),
        }
    }

    fn decode_edit_content(bytes: &[u8]) -> hopnet_photos::envelopes::PhotoEditContentPayload {
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .unwrap()
            .0
    }

    // Impact: this is the failure the metadata_access field exists to
    // prevent, and it is silent at write time — a member's stored wrap
    // would unwrap to the OLD key, and the AEAD only fails on read, long
    // after the transaction committed.
    // Should: encrypt the new metadata under a fresh key and ship wraps that
    // actually open it.
    #[tokio::test]
    async fn edit_metadata_round_trips_under_the_fresh_key() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let metadata = edit_metadata();
        let data: &'static [u8] = &[0x77; 100_000];
        let req = edit_req(
            vec![edited_resource(ResourceKind::Edited, data)],
            Vec::new(),
            Some(&metadata),
        );
        publish_photo_edit(&dispatch, req).await.unwrap();

        let log = dispatch.log.lock().unwrap();
        let (function, payload_bytes) = &log.submits[0];
        assert_eq!(function, "photo_edit_content");
        let decoded = decode_edit_content(payload_bytes);
        let entry = &decoded.entries[0];
        assert_eq!(entry.metadata_access.len(), 1, "one member, one wrap");

        let secret = hopnet_storage::x25519_dalek::StaticSecret::from([0xAB; 32]);
        let recipient = StaticRecipient(secret);
        let key = unwrap_metadata_key(
            &entry.photo_id,
            &entry.metadata_access[0].ephemeral_pubkey,
            &entry.metadata_access[0].encrypted_metadata_key,
            &recipient,
        )
        .unwrap();
        let decrypted = decrypt_metadata(
            &key,
            entry.metadata_nonce.as_ref().unwrap(),
            entry.encrypted_metadata.as_ref().unwrap(),
        )
        .unwrap();
        let round = PhotoMetadata::from_json(&decrypted).unwrap();
        assert_eq!(round.width, Some(480), "the crop's new dimensions");
    }

    // Should: upload only the resources the edit names.
    // Should not: touch the original, which an edit never replaces.
    #[tokio::test]
    async fn edit_uploads_only_the_resources_it_names() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let edited: &'static [u8] = &[0x11; 100_000];
        let thumb: &'static [u8] = &[0x22; 4_096];
        let req = edit_req(
            vec![
                edited_resource(ResourceKind::Edited, edited),
                edited_resource(ResourceKind::ThumbnailSmall, thumb),
            ],
            Vec::new(),
            None,
        );
        publish_photo_edit(&dispatch, req).await.unwrap();

        let log = dispatch.log.lock().unwrap();
        assert_eq!(log.uploads.len(), 2);
        let decoded = decode_edit_content(&log.submits[0].1);
        let kinds: Vec<i32> = decoded.entries[0]
            .resources
            .iter()
            .map(|r| r.resource_type)
            .collect();
        assert_eq!(kinds, vec![1, 5]);
        assert!(
            decoded.entries[0].encrypted_metadata.is_none(),
            "no metadata change, no ciphertext"
        );
        assert!(decoded.entries[0].metadata_access.is_empty());
    }

    // Impact: after a photo publishes, its blobs are evicted from the local
    // spool — a revert that had to re-upload the original could not, because
    // those bytes are gone. Removal is the only expressible answer.
    // Should: submit a removal with no upload at all.
    #[tokio::test]
    async fn removal_only_edit_uploads_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let req = edit_req(Vec::new(), vec![ResourceKind::Edited], None);
        publish_photo_edit(&dispatch, req).await.unwrap();

        let log = dispatch.log.lock().unwrap();
        assert_eq!(log.uploads.len(), 0);
        assert_eq!(log.submits[0].0, "photo_edit_content");
        let decoded = decode_edit_content(&log.submits[0].1);
        assert!(decoded.entries[0].resources.is_empty());
        assert_eq!(decoded.entries[0].remove_resources, vec![1]);
    }

    // Should: route a metadata-only refresh to photo_edit_metadata.
    #[tokio::test]
    async fn metadata_only_edit_submits_photo_edit_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let metadata = edit_metadata();
        let req = edit_req(Vec::new(), Vec::new(), Some(&metadata));
        publish_photo_edit(&dispatch, req).await.unwrap();

        let log = dispatch.log.lock().unwrap();
        assert_eq!(log.uploads.len(), 0);
        let (function, payload_bytes) = &log.submits[0];
        assert_eq!(function, "photo_edit_metadata");
        let (decoded, _): (hopnet_photos::envelopes::PhotoEditMetadataPayload, _) =
            bincode::serde::decode_from_slice(payload_bytes, bincode::config::standard()).unwrap();
        assert_eq!(decoded.entries[0].metadata_access.len(), 1);
    }

    // Should not: reach the network for an edit that changes nothing.
    #[tokio::test]
    async fn rejects_an_edit_that_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let req = edit_req(Vec::new(), Vec::new(), None);
        let err = publish_photo_edit(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::EmptyEdit)
        ));
        assert_eq!(dispatch.log.lock().unwrap().submits.len(), 0);
    }

    // Should not: accept a kind that is both replaced and removed — the
    // handler's upsert and delete would race on execution order.
    #[tokio::test]
    async fn rejects_a_kind_that_is_both_edited_and_removed() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let data: &'static [u8] = &[0x33; 100_000];
        let req = edit_req(
            vec![edited_resource(ResourceKind::Edited, data)],
            vec![ResourceKind::Edited],
            None,
        );
        let err = publish_photo_edit(&dispatch, req).await.unwrap_err();
        assert!(matches!(
            err,
            PhotosCoreError::InvalidPublishRequest(PublishValidationError::EditedAndRemoved(
                ResourceKind::Edited
            ))
        ));
        assert_eq!(dispatch.log.lock().unwrap().uploads.len(), 0);
    }

    // Impact: the uploaded ids are reconciliation candidates the caller owns
    // — losing them on a mid-edit failure strands bytes in the substrate
    // with nothing pointing at them.
    // Should: report every blob that landed before the submit failed.
    #[tokio::test]
    async fn partial_edit_reports_uploaded_blob_ids() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir).with_submit_fail();
        let data: &'static [u8] = &[0x44; 100_000];
        let req = edit_req(
            vec![edited_resource(ResourceKind::Edited, data)],
            Vec::new(),
            None,
        );
        let err = publish_photo_edit(&dispatch, req).await.unwrap_err();
        match err {
            PhotosCoreError::PartialPublish {
                uploaded_blob_ids, ..
            } => assert_eq!(uploaded_blob_ids.len(), 1),
            other => panic!("expected PartialPublish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fragment_meta_converts_blob_id_correctly() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = publish_req(&asset, vec![(ResourceKind::Original, test_stream(data))]);
        publish_photo_add(&dispatch, req).await.unwrap();

        let log = dispatch.log.lock().unwrap();
        let (_, payload_bytes) = &log.submits[0];
        let (decoded, _): (hopnet_photos::envelopes::PhotoAddPayload, _) =
            bincode::serde::decode_from_slice(payload_bytes, bincode::config::standard()).unwrap();
        let op = &decoded.entries[0].resources[0].op;
        assert!(!op.fragments.is_empty());
        for f in &op.fragments {
            assert_eq!(f.blob_id, op.blob_id);
        }
    }

    #[tokio::test]
    async fn first_upload_failure_has_no_partial_publish() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir).with_upload_fail_after(0);
        let asset = test_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = publish_req(&asset, vec![(ResourceKind::Original, test_stream(data))]);
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        assert!(!matches!(err, PhotosCoreError::PartialPublish { .. }));
        assert_eq!(dispatch.log.lock().unwrap().submits.len(), 0);
    }

    #[tokio::test]
    async fn second_upload_failure_reports_first_as_partial() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir).with_upload_fail_after(1);
        let asset = two_resource_asset();
        let data: &'static [u8] = &[0x42; 100_000];
        let req = publish_req(
            &asset,
            vec![
                (ResourceKind::Original, test_stream(data)),
                (ResourceKind::Edited, test_stream(data)),
            ],
        );
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        match err {
            PhotosCoreError::PartialPublish {
                uploaded_blob_ids, ..
            } => {
                assert_eq!(uploaded_blob_ids.len(), 1);
            }
            e => panic!("expected PartialPublish, got {e:?}"),
        }
        assert_eq!(dispatch.log.lock().unwrap().submits.len(), 0);
    }

    #[tokio::test]
    async fn submit_failure_yields_partial_publish() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir).with_submit_fail();
        let asset = test_asset();
        let photo_id = CustomUUID::new(None);
        let data: &'static [u8] = &[0x42; 100_000];
        let req = PublishRequest {
            asset: &asset,
            photo_id: photo_id.clone(),
            library_id: None,
            byte_sources: vec![(ResourceKind::Original, test_stream(data))],
            cloud_fingerprint: None,
        };
        let err = publish_photo_add(&dispatch, req).await.unwrap_err();
        match err {
            PhotosCoreError::PartialPublish {
                photo_id: pid,
                uploaded_blob_ids,
                ..
            } => {
                assert_eq!(pid, photo_id);
                assert_eq!(uploaded_blob_ids.len(), 1);
            }
            e => panic!("expected PartialPublish, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn caller_photo_id_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let dispatch = MockDispatch::new(dir);
        let asset = test_asset();
        let photo_id = CustomUUID::retention_cutoff(5000);
        let data: &'static [u8] = &[0x42; 100_000];
        let req = PublishRequest {
            asset: &asset,
            photo_id: photo_id.clone(),
            library_id: None,
            byte_sources: vec![(ResourceKind::Original, test_stream(data))],
            cloud_fingerprint: None,
        };
        let outcome = publish_photo_add(&dispatch, req).await.unwrap();
        assert_eq!(outcome.photo_id, photo_id);
        assert_ne!(outcome.operation_id, photo_id);
    }
}
