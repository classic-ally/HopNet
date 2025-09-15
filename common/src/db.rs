// Shared database types for FileProvider
use serde::{Deserialize, Serialize};
use typeshare::typeshare;

/// Inode type - file or folder
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
#[typeshare]
pub enum InodeType {
    File,
    Folder,
}