use aes_siv::aead::generic_array::GenericArray;
use hopnet::db::types::CustomUUID;
use hopnet::types::Blake3Hash;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

pub mod genesis;
pub mod keys;
pub mod population;

pub struct FixtureContext {
    /// SIV key for path encryption/decryption
    pub siv_key: Option<aes_siv::Key<aes_siv::siv::Aes256Siv>>,
    /// SIV nonce for path encryption/decryption
    pub siv_nonce: Option<GenericArray<u8, aes_siv::aead::consts::U16>>,
    /// Data block IDs created during population
    pub data_block_ids: Vec<CustomUUID>,
    /// All fragment hashes created during population
    pub fragment_hashes: Vec<Blake3Hash>,
    /// Device token IDs
    pub device_ids: Vec<CustomUUID>,
    /// Share IDs
    pub share_ids: Vec<CustomUUID>,
    /// Committed nonce UUIDs
    pub committed_nonces: Vec<CustomUUID>,
    /// Block hashes for blocks 1-5 (index 0 = height 1)
    pub block_hashes: Vec<Blake3Hash>,
    /// Genesis block hash
    pub genesis_hash: Blake3Hash,
    /// Root folder inode ID (user 0)
    pub root_folder_id: Option<CustomUUID>,
    /// File inode IDs (user 0)
    pub file_ids: Vec<CustomUUID>,
    /// Encrypted root path
    pub encrypted_root: Option<String>,
    /// Base time for metric timestamps (used by capture_all for time-shift)
    pub metrics_base_time: Option<chrono::DateTime<chrono::Utc>>,
}

pub fn seed_all(pool: &Pool<SqliteConnectionManager>) -> FixtureContext {
    let mut ctx = FixtureContext {
        siv_key: None,
        siv_nonce: None,
        data_block_ids: Vec::new(),
        fragment_hashes: Vec::new(),
        device_ids: Vec::new(),
        share_ids: Vec::new(),
        committed_nonces: Vec::new(),
        block_hashes: Vec::new(),
        genesis_hash: Blake3Hash::from_bytes([0u8; 32]),
        root_folder_id: None,
        file_ids: Vec::new(),
        encrypted_root: None,
        metrics_base_time: None,
    };

    genesis::setup_genesis(pool);
    population::populate(pool, &mut ctx);

    ctx
}
