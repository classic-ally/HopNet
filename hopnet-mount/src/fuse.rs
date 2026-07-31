//! Thin fuser adapter over `vfs::MountCore` (linux-only).
//!
//! The async bridge decided in RFC-018 S1: fuser's session loop calls the
//! sync callbacks below; each clones its Arcs and spawns onto the daemon's
//! tokio runtime, moving the owned Reply into the task. The session loop
//! never blocks on the node; concurrency is bounded by the kernel's
//! outstanding-request window. Errno mapping lives here and nowhere else.

use std::ffi::OsStr;
use std::sync::Arc;
use std::time::Duration;

use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, Generation, INodeNo, OpenFlags,
    ReplyAttr, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};

use crate::transport::ItemKind;
use crate::vfs::{CoreError, MountCore, NodeAttr};

/// Kernel-side entry/attr TTL. Generous per RFC-018's cache-until-poked
/// policy; S4's invalidation makes staleness poke-bounded rather than
/// TTL-bounded.
const KERNEL_TTL: Duration = Duration::from_secs(60);

pub struct HopFs {
    core: Arc<MountCore>,
    rt: tokio::runtime::Handle,
    uid: u32,
    gid: u32,
    /// Passthrough armed (S9): requires the CLI not disabling it, the
    /// kernel negotiating FUSE_PASSTHROUGH at init, AND no EPERM yet —
    /// the backing ioctl needs CAP_SYS_ADMIN, so the first refusal
    /// disarms for the session (probe ladder, never an error).
    passthrough_allowed: bool,
    passthrough: Arc<std::sync::atomic::AtomicBool>,
    /// Live backing registrations by file handle. The BackingId must
    /// outlive the kernel's use of the fh (drop => backing-close ioctl;
    /// dropping early makes reads EIO), and the Backing's pin keeps
    /// eviction away from the file the kernel is reading.
    #[allow(clippy::type_complexity)]
    backings: Arc<
        std::sync::Mutex<
            std::collections::HashMap<u64, (Arc<fuser::BackingId>, crate::cache::Backing)>,
        >,
    >,
}

impl HopFs {
    pub fn new(core: Arc<MountCore>, rt: tokio::runtime::Handle, allow_passthrough: bool) -> Self {
        HopFs {
            core,
            rt,
            uid: rustix::process::getuid().as_raw(),
            gid: rustix::process::getgid().as_raw(),
            passthrough_allowed: allow_passthrough,
            passthrough: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            backings: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Attr conversion as a value, movable into spawned tasks.
    fn attr_of(&self) -> impl Fn(&NodeAttr) -> FileAttr + Send + 'static {
        let (uid, gid) = (self.uid, self.gid);
        move |node: &NodeAttr| {
            let (kind, perm, size, nlink) = match node.item.kind {
                ItemKind::File { size } => (FileType::RegularFile, 0o644, size, 1),
                ItemKind::Folder => (FileType::Directory, 0o755, 0, 2),
            };
            FileAttr {
                ino: INodeNo(node.ino),
                size,
                blocks: size.div_ceil(512),
                atime: node.item.modified,
                mtime: node.item.modified,
                ctime: node.item.modified,
                crtime: node.item.created,
                kind,
                perm,
                nlink,
                uid,
                gid,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            }
        }
    }
}

/// fuser-backed kernel invalidation (RFC-018 S4). Obtained from
/// `BackgroundSession::notifier()` after mount; Clone+Send+Sync per fuser.
/// Errors are logged, not propagated — ENOENT ("kernel didn't have it
/// cached") is the common, harmless case.
pub struct FuserInvalidator(pub fuser::Notifier);

impl crate::watch::KernelInvalidator for FuserInvalidator {
    fn inval_entry(&self, parent_ino: u64, name: &str) {
        if let Err(e) = self.0.inval_entry(INodeNo(parent_ino), std::ffi::OsStr::new(name)) {
            tracing::debug!("inval_entry({parent_ino}, {name}): {e}");
        }
    }
    fn inval_inode(&self, ino: u64) {
        // Whole-inode: attrs + full data range.
        if let Err(e) = self.0.inval_inode(INodeNo(ino), 0, -1) {
            tracing::debug!("inval_inode({ino}): {e}");
        }
    }
}

fn errno(e: &CoreError) -> Errno {
    match e {
        CoreError::NotFound => Errno::ENOENT,
        CoreError::NotADirectory => Errno::ENOTDIR,
        CoreError::IsADirectory => Errno::EISDIR,
        CoreError::AlreadyExists => Errno::EEXIST,
        CoreError::NotEmpty => Errno::ENOTEMPTY,
        CoreError::StaleHandle => Errno::EBADF,
        CoreError::Staging(_) => Errno::EIO,
        CoreError::Transport(_) => Errno::EIO,
        CoreError::Cache(_) => Errno::EIO,
    }
}

impl Filesystem for HopFs {
    fn init(
        &mut self,
        _req: &Request,
        config: &mut fuser::KernelConfig,
    ) -> std::io::Result<()> {
        if !self.passthrough_allowed {
            return Ok(());
        }
        // The negotiation doubles as the kernel probe: pre-6.9 kernels
        // (or CONFIG_FUSE_PASSTHROUGH=n) don't advertise the flag and
        // add_capabilities errs — fall back, never fail the mount.
        match config.add_capabilities(fuser::InitFlags::FUSE_PASSTHROUGH) {
            Ok(()) => {
                // 1 = our backing files live on a normal filesystem;
                // depth only matters for stacking FUSE on FUSE.
                if let Err(e) = config.set_max_stack_depth(1) {
                    tracing::info!("passthrough disabled: max_stack_depth rejected ({e})");
                } else {
                    self.passthrough
                        .store(true, std::sync::atomic::Ordering::Release);
                    tracing::info!("FUSE passthrough negotiated");
                }
            }
            Err(_) => {
                tracing::info!(
                    "kernel lacks FUSE_PASSTHROUGH (needs 6.9+); daemon-mediated reads"
                );
            }
        }
        Ok(())
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str().map(String::from) else {
            // Drive names are UTF-8 by construction; nothing non-UTF-8 exists.
            reply.error(Errno::ENOENT);
            return;
        };
        let core = self.core.clone();
        let attr_of = self.attr_of();
        self.rt.spawn(async move {
            match core.lookup(parent.0, &name).await {
                Ok(node) => reply.entry(&KERNEL_TTL, &attr_of(&node), Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let core = self.core.clone();
        let attr_of = self.attr_of();
        self.rt.spawn(async move {
            match core.getattr(ino.0).await {
                Ok(node) => {
                    let mut attr = attr_of(&node);
                    // Dirty write sessions overlay the freshest size.
                    if let Some(staged) = core.staged_size(ino.0).await {
                        attr.size = staged;
                    }
                    reply.attr(&KERNEL_TTL, &attr);
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn opendir(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        let core = self.core.clone();
        self.rt.spawn(async move {
            match core.opendir(ino.0).await {
                Ok(fh) => reply.opened(FileHandle(fh), fuser::FopenFlags::empty()),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn readdir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let core = self.core.clone();
        self.rt.spawn(async move {
            let entries = match core.dir_entries(fh.0) {
                Ok(entries) => entries,
                Err(e) => {
                    reply.error(errno(&e));
                    return;
                }
            };
            for (i, entry) in entries.iter().enumerate().skip(offset as usize) {
                let kind = if entry.is_dir {
                    FileType::Directory
                } else {
                    FileType::RegularFile
                };
                // Offset carried per entry = resume point AFTER this entry.
                if reply.add(INodeNo(entry.ino), (i + 1) as u64, kind, &entry.name) {
                    break;
                }
            }
            reply.ok();
        });
    }

    fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        self.core.releasedir(fh.0);
        reply.ok();
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let core = self.core.clone();
        let write_intent = !matches!(flags.acc_mode(), fuser::OpenAccMode::O_RDONLY);
        let passthrough = self.passthrough.clone();
        let backings = self.backings.clone();
        self.rt.spawn(async move {
            // O_TRUNC never reaches open (kernel sends setattr(size=0)).
            let result = if write_intent {
                core.open_rw(ino.0, false).await
            } else {
                core.open(ino.0).await
            };
            let fh = match result {
                Ok(fh) => fh,
                Err(e) => return reply.error(errno(&e)),
            };

            // Passthrough (S9): read-only opens of fully-cached blobs
            // hand the kernel a backing fd — reads then bypass the
            // daemon entirely. Every failure falls through to the
            // daemon-mediated reply; correctness never depends on this.
            if !write_intent && passthrough.load(std::sync::atomic::Ordering::Acquire) {
                if let Some(backing) = core.backing_for(fh) {
                    match reply.open_backing(&backing.file) {
                        Ok(id) => {
                            let id = Arc::new(id);
                            // Insert BEFORE replying so even an
                            // instant release finds the entry.
                            backings
                                .lock()
                                .expect("backings poisoned")
                                .insert(fh, (id.clone(), backing));
                            return reply.opened_passthrough(
                                FileHandle(fh),
                                fuser::FopenFlags::empty(),
                                &id,
                            );
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                            // No CAP_SYS_ADMIN: disarm for the session,
                            // one info line, permanent fallback.
                            passthrough.store(false, std::sync::atomic::Ordering::Release);
                            tracing::info!(
                                "passthrough needs CAP_SYS_ADMIN (see the NixOS module's \
                                 allowPassthrough); daemon-mediated reads"
                            );
                        }
                        Err(e) => tracing::debug!("open_backing failed: {e}"),
                    }
                }
            }
            reply.opened(FileHandle(fh), fuser::FopenFlags::empty())
        });
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let Some(name) = name.to_str().map(String::from) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let core = self.core.clone();
        let attr_of = self.attr_of();
        self.rt.spawn(async move {
            match core.create(parent.0, &name).await {
                Ok((node, fh)) => reply.created(
                    &KERNEL_TTL,
                    &attr_of(&node),
                    Generation(0),
                    FileHandle(fh),
                    fuser::FopenFlags::empty(),
                ),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str().map(String::from) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let core = self.core.clone();
        let attr_of = self.attr_of();
        self.rt.spawn(async move {
            match core.mkdir(parent.0, &name).await {
                Ok(node) => reply.entry(&KERNEL_TTL, &attr_of(&node), Generation(0)),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let core = self.core.clone();
        let data = data.to_vec();
        self.rt.spawn(async move {
            match core.write(fh.0, offset, &data).await {
                Ok(written) => reply.written(written),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        fh: Option<FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let core = self.core.clone();
        let attr_of = self.attr_of();
        self.rt.spawn(async move {
            // Size is the only attribute we persist; mode/uid/times are
            // synthesized (accepted and ignored — keeps cp/rsync happy).
            if let Some(size) = size {
                let result = match fh {
                    Some(fh) => core.truncate(fh.0, size).await,
                    None => match core.open_rw(ino.0, size == 0).await {
                        // Truncate without an open handle: synthesize a
                        // session; release uploads in the background.
                        Ok(tmp_fh) => {
                            let r = core.truncate(tmp_fh, size).await;
                            core.release(tmp_fh);
                            r
                        }
                        Err(e) => Err(e),
                    },
                };
                if let Err(e) = result {
                    reply.error(errno(&e));
                    return;
                }
            }
            match core.getattr(ino.0).await {
                Ok(node) => {
                    let mut attr = attr_of(&node);
                    if let Some(staged) = core.staged_size(ino.0).await {
                        attr.size = staged;
                    }
                    reply.attr(&KERNEL_TTL, &attr);
                }
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        let core = self.core.clone();
        self.rt.spawn(async move {
            // The strict tier: returns only after decided upload.
            match core.fsync(fh.0).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: ReplyEmpty,
    ) {
        // close(2) semantics: durability is fsync's job; release uploads
        // in the background off durable staging.
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: fuser::RenameFlags,
        reply: ReplyEmpty,
    ) {
        let (Some(name), Some(newname)) = (
            name.to_str().map(String::from),
            newname.to_str().map(String::from),
        ) else {
            reply.error(Errno::EINVAL);
            return;
        };
        let core = self.core.clone();
        self.rt.spawn(async move {
            match core.rename(parent.0, &name, newparent.0, &newname).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str().map(String::from) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let core = self.core.clone();
        self.rt.spawn(async move {
            match core.remove(parent.0, &name, false).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str().map(String::from) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let core = self.core.clone();
        self.rt.spawn(async move {
            match core.remove(parent.0, &name, true).await {
                Ok(()) => reply.ok(),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn read(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyData,
    ) {
        let core = self.core.clone();
        self.rt.spawn(async move {
            match core.read(fh.0, offset, size as u64).await {
                // Short only at EOF (fuser zero-fills otherwise, which is
                // exactly the EOF contract we want here).
                Ok(bytes) => reply.data(&bytes),
                Err(e) => reply.error(errno(&e)),
            }
        });
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        // Must run on the runtime: release spawns the background upload.
        let core = self.core.clone();
        let backings = self.backings.clone();
        self.rt.spawn(async move {
            // Passthrough teardown first: dropping the BackingId issues
            // the kernel backing-close ioctl, and dropping the Backing
            // releases the eviction pin. Unlike the S4/S7 notify case
            // (which waits on inode locks whose holders may need OUR
            // reply), the backing ioctls are bounded kernel work with
            // no request-completion dependency — safe inline.
            drop(backings.lock().expect("backings poisoned").remove(&fh.0));
            core.release(fh.0);
            reply.ok();
        });
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Node-side numbers (RFC-018 S8): total = capacity while the mesh
        // tolerates >= 2 failures, used = observed bytes — never local
        // cache state. The core TTL-caches and serves last-known on
        // transport blips, so this arm cannot error.
        let core = self.core.clone();
        self.rt.spawn(async move {
            let info = core.statfs().await;
            const BLOCK: u64 = 4096;
            let blocks = info.total_bytes / BLOCK;
            let free = info.total_bytes.saturating_sub(info.used_bytes) / BLOCK;
            reply.statfs(blocks, free, free, 0, 0, BLOCK as u32, 255, BLOCK as u32);
        });
    }
}
