/// Substrate-level errors for the pure modules (crypto, fragment I/O).
///
/// The host maps these onto its own error taxonomy (the main crate's
/// `FileError`) at the delegation seam — this crate never sees HTTP or
/// database error shapes.
#[derive(Debug)]
pub enum StorageError {
    /// AEAD encrypt/decrypt failure (bad key, truncated or tampered segment).
    Encryption,
    /// Content hash did not match the expected fragment hash.
    HashMismatch,
    /// Fragment file I/O failure.
    Io(std::io::Error),
    /// Failure reading the caller-supplied plaintext source.
    Read(std::io::Error),
    /// Reed-Solomon encode/decode failure.
    Rs,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::Encryption => write!(f, "encryption error"),
            StorageError::HashMismatch => write!(f, "fragment hash mismatch"),
            StorageError::Io(e) => write!(f, "fragment I/O error: {}", e),
            StorageError::Read(e) => write!(f, "source read error: {}", e),
            StorageError::Rs => write!(f, "reed-solomon coding error"),
        }
    }
}

impl std::error::Error for StorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Io(e) | StorageError::Read(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StorageError {
    fn from(e: std::io::Error) -> Self {
        StorageError::Io(e)
    }
}
