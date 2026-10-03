//! The evidence lane (RFC-STORAGE-003 S3/S5): pull evidence off the worker.
//!
//! The pull worker used to await up to three consensus commits for every
//! blob it moved — the blob's belief, its disk truth, the confirmation —
//! each up to the queue's 120 s deadline, before starting the next blob
//! (production, 2026-10-03: ~6 blobs a minute per node). Now the worker
//! fetches, stores and content-verifies, hands `(blob, height, present)`
//! to this lane and moves on. The lane buffers per node and flushes when
//! a page is full or its oldest entry is a minute old, with the rolling
//! sweep's page size and age, always in this order and one transaction in
//! flight at a time:
//!
//! 1. belief: one `self_check_fragments` page per 8,192 hashes, the union
//!    of the buffered blobs' blob-scoped reports, computed at flush time
//!    (after the marks landed);
//! 2. truth: `attest_fragments` pages, each stamped with the lowest height
//!    any of its hashes was observed at (read before the rehash);
//! 3. confirm: one `ConfirmPlacement` page for every buffered blob whose
//!    goal evidence is complete — whichever responsible attests last
//!    proposes, so sparse ready blobs no longer wait on the tick's sample.
//!
//! Failure tolerance is the sweep's: a failed belief page does not block
//! truth, a failed page does not stop the next, and anything lost is
//! re-covered by the next pull or the sweep's rotation. No payload changes:
//! the same three transactions, batched.

use crate::lifecycle::{ConfirmPlacement, PlacementConfirmation, CONFIRM_TX_FN};
use crate::sweep::PageBuffer;
use crate::traits::{StateReader, TxSubmitter};
use crate::types::BlobId;
use hopnet_common::Blake3Hash;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc;

use super::policy;

/// What a pull hands the lane: the blob, the height read BEFORE its
/// fragments were content-verified, and the hashes that verified.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceItem {
    pub blob_id: BlobId,
    pub height: u64,
    pub present: Vec<Blake3Hash>,
}

/// Sender side of the lane. Cheap to clone.
#[derive(Clone)]
pub struct EvidenceLane {
    tx: mpsc::UnboundedSender<EvidenceItem>,
}

impl EvidenceLane {
    /// A lane whose items go to the returned receiver (tests; `spawn`
    /// wraps this with the flushing task).
    pub fn channel() -> (Self, mpsc::UnboundedReceiver<EvidenceItem>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (EvidenceLane { tx }, rx)
    }

    /// Queue a blob's evidence. Never blocks; false when the lane is gone.
    pub fn push(&self, item: EvidenceItem) -> bool {
        self.tx.send(item).is_ok()
    }

    /// Spawn the lane's task on `rt`.
    pub fn spawn<S, X>(state: Arc<S>, submitter: Arc<X>, rt: &tokio::runtime::Handle) -> Self
    where
        S: StateReader + 'static,
        X: TxSubmitter + 'static,
    {
        let (lane, mut rx) = Self::channel();
        rt.spawn(async move {
            let mut buf = LaneBuffer::default();
            loop {
                let open = tokio::select! {
                    item = rx.recv() => match item {
                        Some(item) => {
                            buf.push(item, unix_now());
                            true
                        }
                        None => false,
                    },
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => true,
                };
                // Take everything already waiting before deciding to flush.
                while let Ok(item) = rx.try_recv() {
                    buf.push(item, unix_now());
                }
                if !buf.is_empty() && (!open || buf.due(unix_now())) {
                    let done = flush(state.as_ref(), submitter.as_ref(), &mut buf).await;
                    tracing::debug!(?done, "evidence lane: flushed");
                }
                if !open {
                    break;
                }
            }
        });
        lane
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// The lane's buffered work: the blobs to report belief and confirmation
/// for, and their verified hashes waiting for an attestation page.
#[derive(Debug, Default)]
pub struct LaneBuffer {
    blobs: Vec<BlobId>,
    seen: HashSet<BlobId>,
    truth: PageBuffer,
    /// When the oldest buffered blob arrived (unix seconds).
    opened_at: Option<u64>,
}

impl LaneBuffer {
    pub fn push(&mut self, item: EvidenceItem, now: u64) {
        if self.seen.insert(item.blob_id.clone()) {
            self.blobs.push(item.blob_id);
        }
        self.truth.push(item.height, now, item.present);
        self.opened_at.get_or_insert(now);
    }

    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty() && self.truth.is_empty()
    }

    /// A full attestation page is waiting, or the oldest entry has waited
    /// the sweep's buffer age.
    pub fn due(&self, now: u64) -> bool {
        self.truth.len() >= policy::ATTEST_PAGE_SIZE
            || self
                .opened_at
                .is_some_and(|t| now.saturating_sub(t) >= policy::EVIDENCE_MAX_AGE_SECS)
    }
}

/// What one flush submitted.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LaneFlush {
    pub belief_pages: usize,
    pub belief_failed: usize,
    pub truth_pages: usize,
    pub truth_failed: usize,
    pub confirmed: usize,
    pub confirm_failed: usize,
}

/// Page the buffer out: belief, then truth, then confirmation. Returns
/// what was submitted; the buffer is empty afterwards (anything that
/// failed is the next pull's or the sweep's to re-cover).
pub async fn flush<S, X>(state: &S, submitter: &X, buf: &mut LaneBuffer) -> LaneFlush
where
    S: StateReader,
    X: TxSubmitter,
{
    let mut done = LaneFlush::default();
    let page = policy::ATTEST_PAGE_SIZE;
    let blobs = std::mem::take(&mut buf.blobs);
    buf.seen.clear();
    buf.opened_at = None;
    let Some(me) = state.local_node_id() else {
        tracing::warn!("evidence lane: node id not set; dropping a flush");
        buf.truth = PageBuffer::default();
        return done;
    };

    // (1) Belief: this node's held classes of the buffered blobs that
    // consensus has no row for, computed now that the marks have landed.
    let mut added: Vec<Blake3Hash> = Vec::new();
    let mut seen_hashes: HashSet<Blake3Hash> = HashSet::new();
    let mut belief_height = u64::MAX;
    for blob_id in &blobs {
        match state.blob_self_check_report(blob_id) {
            Ok(report) => {
                if !report.fragments_added.is_empty() {
                    belief_height = belief_height.min(report.self_verified_height);
                }
                added.extend(
                    report
                        .fragments_added
                        .into_iter()
                        .filter(|h| seen_hashes.insert(*h)),
                );
            }
            Err(e) => tracing::debug!("evidence lane: belief for {blob_id}: {e}"),
        }
    }
    for chunk in added.chunks(page) {
        let report = crate::types::SelfCheckFragments {
            node_id: me,
            self_verified_height: belief_height,
            previous_count: 0,
            fragments_added: chunk.to_vec(),
            fragments_removed: Vec::new(),
        };
        if submit(submitter, policy::SELF_CHECK_FN, &report).await {
            done.belief_pages += 1;
        } else {
            done.belief_failed += 1;
        }
    }

    // (2) Truth, at the lowest height each page's hashes were seen at.
    while let Some((height, present)) = buf.truth.take_page(page) {
        let attestation = crate::types::FragmentAttestation {
            node_id: me,
            height,
            present,
            suspect: Vec::new(),
        };
        if submit(submitter, policy::ATTEST_FN, &attestation).await {
            done.truth_pages += 1;
        } else {
            done.truth_failed += 1;
        }
    }

    // (3) Confirm every buffered blob whose goal evidence is now complete.
    let ready: Vec<PlacementConfirmation> = blobs
        .iter()
        .filter_map(|blob_id| match state.confirm_ready(blob_id) {
            Ok(Some(height)) => Some(PlacementConfirmation {
                blob_id: blob_id.clone(),
                height,
            }),
            Ok(None) => None,
            Err(e) => {
                tracing::debug!("evidence lane: confirm check for {blob_id}: {e}");
                None
            }
        })
        .collect();
    for chunk in ready.chunks(policy::CONFIRM_PAGE_SIZE) {
        let payload = ConfirmPlacement {
            confirmations: chunk.to_vec(),
        };
        if submit(submitter, CONFIRM_TX_FN, &payload).await {
            done.confirmed += chunk.len();
        } else {
            done.confirm_failed += chunk.len();
        }
    }
    done
}

/// Encode and submit one page; logs and reports false on any failure.
async fn submit<X: TxSubmitter, P: serde::Serialize>(
    submitter: &X,
    function: &'static str,
    payload: &P,
) -> bool {
    let encoded = match bincode::serde::encode_to_vec(payload, bincode::config::standard()) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!("evidence lane: {function} encode: {e}");
            return false;
        }
    };
    match submitter.submit(function, encoded).await {
        Ok(()) => true,
        Err(e) => {
            tracing::warn!("evidence lane: {function} page failed: {e:?}");
            false
        }
    }
}
