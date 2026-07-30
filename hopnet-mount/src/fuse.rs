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
}

impl HopFs {
    pub fn new(core: Arc<MountCore>, rt: tokio::runtime::Handle) -> Self {
        HopFs {
            core,
            rt,
            uid: rustix::process::getuid().as_raw(),
            gid: rustix::process::getgid().as_raw(),
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

fn errno(e: &CoreError) -> Errno {
    match e {
        CoreError::NotFound => Errno::ENOENT,
        CoreError::NotADirectory => Errno::ENOTDIR,
        CoreError::StaleHandle => Errno::EBADF,
        CoreError::Transport(_) => Errno::EIO,
    }
}

impl Filesystem for HopFs {
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
                Ok(node) => reply.attr(&KERNEL_TTL, &attr_of(&node)),
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

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        // Placeholder numbers; node-side tolerance-constrained capacity
        // lands in S8 (issue #24).
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 4096);
    }
}
