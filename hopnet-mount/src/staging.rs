//! Durable write-back staging (RFC-018 S7).
//!
//! Unlike the content cache (ephemeral, punchable), staging holds DIRTY
//! data: bytes written through the mount that have not yet been uploaded.
//! It survives daemon restarts — `scan()` recovers orphans at startup and
//! re-runs the upload path. Layout: `{dir}/{uuid}.data` (content) +
//! `{dir}/{uuid}.json` (StagedMeta), written meta-last so a scan never
//! sees a meta without its data.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::transport::Height;
use hopnet_common::CustomUUID;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StagedMeta {
    /// The node inode this content belongs to (files are created empty
    /// on the node BEFORE content stages, so this always exists).
    pub inode_id: CustomUUID,
    /// The item height the write session was based on — conflict
    /// detection compares this against the item at upload time.
    pub base_height: Height,
}

pub struct Staging {
    dir: PathBuf,
}

/// One dirty file's staging pair. Cloneable handle; the underlying file
/// is shared.
#[derive(Clone)]
pub struct StagedFile {
    pub meta: StagedMeta,
    id: String,
    dir: PathBuf,
    file: Arc<std::fs::File>,
}

pub struct Recovered {
    pub staged: StagedFile,
}

impl Staging {
    pub fn new(dir: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&dir)?;
        std::fs::create_dir_all(dir.join("orphaned"))?;
        Ok(Staging { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Open a fresh staging pair for a write session on `inode_id`.
    pub fn begin(&self, meta: StagedMeta) -> std::io::Result<StagedFile> {
        let id = CustomUUID::new(None).to_string();
        let data_path = self.dir.join(format!("{id}.data"));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&data_path)?;
        // Meta written AFTER data exists (scan order guarantee).
        let meta_json = serde_json::to_vec(&meta).expect("meta serializes");
        std::fs::write(self.dir.join(format!("{id}.json")), meta_json)?;
        Ok(StagedFile {
            meta,
            id,
            dir: self.dir.clone(),
            file: Arc::new(file),
        })
    }

    /// Recover staging pairs left by a previous daemon run.
    pub fn scan(&self) -> Vec<Recovered> {
        let mut recovered = Vec::new();
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return recovered;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let Some(id) = path.file_stem().and_then(|s| s.to_str()).map(String::from) else {
                continue;
            };
            let Ok(meta_bytes) = std::fs::read(&path) else {
                continue;
            };
            let Ok(meta) = serde_json::from_slice::<StagedMeta>(&meta_bytes) else {
                tracing::warn!("unreadable staging meta {path:?}; leaving in place");
                continue;
            };
            let data_path = self.dir.join(format!("{id}.data"));
            let Ok(file) = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&data_path)
            else {
                tracing::warn!("staging meta without data: {path:?}");
                continue;
            };
            recovered.push(Recovered {
                staged: StagedFile {
                    meta,
                    id,
                    dir: self.dir.clone(),
                    file: Arc::new(file),
                },
            });
        }
        recovered
    }
}

impl StagedFile {
    pub async fn write_at(&self, offset: u64, data: Vec<u8>) -> std::io::Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            file.write_all_at(&data, offset)
        })
        .await
        .expect("staging write task")
    }

    pub async fn read_at(&self, offset: u64, len: u64) -> std::io::Result<Vec<u8>> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || {
            use std::os::unix::fs::FileExt;
            let size = file.metadata()?.len();
            if offset >= size {
                return Ok(Vec::new());
            }
            let len = len.min(size - offset) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact_at(&mut buf, offset)?;
            Ok(buf)
        })
        .await
        .expect("staging read task")
    }

    pub async fn set_len(&self, len: u64) -> std::io::Result<()> {
        let file = self.file.clone();
        tokio::task::spawn_blocking(move || file.set_len(len))
            .await
            .expect("staging truncate task")
    }

    pub fn size(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Streaming read of the whole staged content for upload.
    pub fn byte_source(&self) -> crate::transport::ByteSource {
        let file = self.file.clone();
        Box::pin(async_stream::stream! {
            const CHUNK: u64 = 1 << 20;
            let mut offset = 0u64;
            loop {
                let file = file.clone();
                let chunk = tokio::task::spawn_blocking(move || {
                    use std::os::unix::fs::FileExt;
                    let size = file.metadata()?.len();
                    if offset >= size {
                        return Ok(Vec::new());
                    }
                    let len = CHUNK.min(size - offset) as usize;
                    let mut buf = vec![0u8; len];
                    file.read_exact_at(&mut buf, offset)?;
                    std::io::Result::Ok(buf)
                })
                .await
                .expect("staging stream task");
                match chunk {
                    Ok(bytes) if bytes.is_empty() => break,
                    Ok(bytes) => {
                        offset += bytes.len() as u64;
                        yield Ok(bytes::Bytes::from(bytes));
                    }
                    Err(e) => {
                        yield Err(e);
                        break;
                    }
                }
            }
        })
    }

    /// Upload succeeded — remove the pair.
    pub fn finish(&self) {
        let _ = std::fs::remove_file(self.dir.join(format!("{}.data", self.id)));
        let _ = std::fs::remove_file(self.dir.join(format!("{}.json", self.id)));
    }

    /// Recovery gave up (target inode gone) — park the pair out of the
    /// scan path so it isn't retried forever, but never delete user data.
    pub fn orphan(&self) {
        let orphaned = self.dir.join("orphaned");
        let _ = std::fs::rename(
            self.dir.join(format!("{}.data", self.id)),
            orphaned.join(format!("{}.data", self.id)),
        );
        let _ = std::fs::rename(
            self.dir.join(format!("{}.json", self.id)),
            orphaned.join(format!("{}.json", self.id)),
        );
    }
}
