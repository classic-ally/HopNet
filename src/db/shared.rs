use super::*;
use std::path::Path;
use std::fs;

/// Get the XDG data directory for storing the database
pub fn get_database_path() -> String {
    let data_dir = std::env::var("XDG_DATA_HOME")
        .unwrap_or_else(|_| format!("{}/.local/share", std::env::var("HOME").unwrap_or_else(|_| ".".to_string())));

    let db_dir = format!("{}/hopnet", data_dir);
    let db_path = format!("{}/database.db", db_dir);
    tracing::info!("Using database path: {}", db_path);
    db_path
}

/// Check if the database file already exists
pub fn database_exists(db_path: &str) -> bool {
    Path::new(db_path).exists()
}

/// Check if the database schema is initialized by checking for critical tables
pub fn is_schema_initialized(db: &PooledConnection<SqliteConnectionManager>) -> Result<bool, DuckdbError> {
    // Check if the critical 'blocks' table exists
    let result = db.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'blocks'",
        [],
        |row| row.get::<_, i64>(0)
    );

    match result {
        Ok(count) => Ok(count > 0),
        Err(_) => Ok(false),
    }
}

/// Ensure the database directory exists
pub fn ensure_database_dir(db_path: &str) -> Result<(), std::io::Error> {
    if let Some(parent) = Path::new(db_path).parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Connection customizer that runs PRAGMAs and registers custom functions on each new connection
#[derive(Debug)]
pub struct SqliteInitializer;

impl r2d2::CustomizeConnection<rusqlite::Connection, rusqlite::Error> for SqliteInitializer {
    fn on_acquire(&self, conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;
            PRAGMA busy_timeout = 5000;
        ")?;
        register_custom_functions(conn)?;
        Ok(())
    }
}

/// Register custom SQL functions needed by queries across the codebase
pub fn register_custom_functions(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    // uuid_extract_timestamp(uuid_text) → INTEGER (NULL-safe: NULL in → NULL out)
    // Parse UUIDv7 hex, extract 48-bit timestamp, return epoch millis
    conn.create_scalar_function("uuid_extract_timestamp", 1, rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let uuid_str: Option<String> = ctx.get(0)?;
        match uuid_str {
            None => Ok(None),
            Some(s) => {
                let hex_only: String = s.replace('-', "");
                if hex_only.len() < 12 {
                    return Ok(Some(0i64));
                }
                match i64::from_str_radix(&hex_only[..12], 16) {
                    Ok(millis) => Ok(Some(millis)),
                    Err(_) => Ok(Some(0i64)),
                }
            }
        }
    })?;

    // reverse(text) → TEXT
    // String reversal for parent-path extraction patterns
    conn.create_scalar_function("reverse", 1, rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let s: String = ctx.get(0)?;
        Ok(s.chars().rev().collect::<String>())
    })?;

    // sqrt(x) → REAL
    conn.create_scalar_function("sqrt", 1, rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let x: f64 = ctx.get(0)?;
        Ok(x.sqrt())
    })?;

    // pow(x, y) → REAL
    conn.create_scalar_function("pow", 2, rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let base: f64 = ctx.get(0)?;
        let exp: f64 = ctx.get(1)?;
        Ok(base.powf(exp))
    })?;

    // log10(x) → REAL
    conn.create_scalar_function("log10", 1, rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC, |ctx| {
        let x: f64 = ctx.get(0)?;
        Ok(x.log10())
    })?;

    Ok(())
}

pub fn initialize(db: PooledConnection<SqliteConnectionManager>) -> Result<(), DuckdbError> {
    db.execute_batch(
        "
            CREATE TABLE sequences (
                name            TEXT PRIMARY KEY,
                next_id         INTEGER NOT NULL
            );

            CREATE TABLE users (
                user_id         INTEGER PRIMARY KEY,
                username        TEXT NOT NULL,
                pubkey          BLOB NOT NULL,
                x25519_pubkey   BLOB NOT NULL,  -- 32 bytes X25519 public key for file access
                encrypted_privkey BLOB NOT NULL, -- nonce || ChaCha20-Poly1305 ciphertext
                key_salt        BLOB NOT NULL,   -- Argon2 salt
                first_name      TEXT,            -- optional first name (max 32 chars)
                last_name       TEXT,            -- optional last name (max 32 chars)
                avatar          BLOB,            -- optional avatar (JPEG, max 128KB)

                CONSTRAINT unique_username UNIQUE (username)
            );

            CREATE TABLE nodes (
                node_id         INTEGER PRIMARY KEY,
                name            TEXT NOT NULL,
                owner           INTEGER NOT NULL,
                pubkey          BLOB NOT NULL,

                FOREIGN KEY (owner) REFERENCES users(user_id)
            );

            -- Common query patterns:
            -- 1. user owns what nodes?
            CREATE INDEX idx_nodes_owner ON nodes(owner);

            -- Consensus architecture
            CREATE TABLE blocks (
                block_hash      BLOB PRIMARY KEY,
                height          INTEGER NOT NULL,
                view_number     INTEGER NOT NULL,
                parent_hash     BLOB,
                transactions    BLOB,

                CONSTRAINT fk_parent_exists FOREIGN KEY (parent_hash) REFERENCES blocks(block_hash)
            );

            -- Common query patterns:
            -- 1. Give me latest blocks, most recent few
            -- 2. Give me blocks for a given view
            -- 3. Look up parent of a block
            CREATE INDEX idx_blocks_height ON blocks(height DESC);
            CREATE INDEX idx_blocks_view ON blocks(view_number);
            CREATE INDEX idx_blocks_parent ON blocks(parent_hash);

            CREATE TABLE quorum_certificates (
                view_number         INTEGER NOT NULL,
                phase               INTEGER NOT NULL CHECK(phase IN (0, 1)),  -- 0=propose, 1=lock
                block_hash          BLOB NOT NULL,
                proposer_signature  BLOB NOT NULL,
                voter_signatures    BLOB,

                PRIMARY KEY (view_number, phase, block_hash),
                FOREIGN KEY (block_hash) REFERENCES blocks(block_hash)
            );

            CREATE INDEX idx_qc_block ON quorum_certificates(block_hash);
            CREATE INDEX idx_view_phase ON quorum_certificates(view_number, phase);

            CREATE TABLE timeout_certificates (
                view_number             INTEGER PRIMARY KEY,    -- View that timed out
                highest_qc_view         INTEGER NOT NULL,       -- QC's view number
                highest_qc_phase        INTEGER NOT NULL CHECK(highest_qc_phase IN (0, 1)),  -- 0=propose, 1=lock
                highest_qc_block_hash   BLOB NOT NULL,          -- QC's block hash
                signatures              BLOB NOT NULL,          -- Timeout vote signatures

                FOREIGN KEY (highest_qc_view, highest_qc_phase, highest_qc_block_hash)
                    REFERENCES quorum_certificates(view_number, phase, block_hash)
            );

            CREATE INDEX idx_tc_view ON timeout_certificates(view_number);
            CREATE INDEX idx_tc_highest_qc ON timeout_certificates(highest_qc_view, highest_qc_phase, highest_qc_block_hash);

            -- Track validators that are acceptable at any given time
            -- Not using views (nodes can be in different views due to network partitions)
            -- Not using timestamps (time sync requirement)
            -- Using height (deterministic, directly tied to the block being committed)
            CREATE TABLE validators (
                effective_height    INTEGER NOT NULL,   -- Height when this validator changes state
                node_id             INTEGER NOT NULL,
                is_active           INTEGER NOT NULL,

                PRIMARY KEY (effective_height, node_id),
                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            -- Common query patterns:
            -- 1. Give me current validators (e.g. latest effective height for leave/rejoin)
            -- 2. For consensus rebuild, give me nodes active at a given height
            CREATE INDEX idx_validator_height ON validators(effective_height DESC); 
            CREATE INDEX idx_validator_active ON validators(effective_height, is_active);

            CREATE TABLE this_node (
                internal_id             INTEGER PRIMARY KEY DEFAULT 1,
                node_id                 INTEGER NOT NULL UNIQUE,
                privkey                 BLOB NOT NULL,

                -- Consensus mechanics
                -- View stored in case of leader change without block written
                -- Block height not stored -> always computable
                current_phase           INTEGER NOT NULL DEFAULT 0 CHECK(current_phase IN (0, 1)),  -- 0=propose, 1=lock
                current_view            INTEGER NOT NULL DEFAULT 0,
                -- Track last view where we issued a timeout vote to prevent conflicting votes
                last_timeout_vote_view  INTEGER DEFAULT 0,
                -- Track last Propose phase vote to prevent double-voting
                last_propose_vote_block_hash BLOB,
                -- Block is prepared when it has a QC
                prepared_block_hash     BLOB,
                -- HotStuff-2 efficiency improvement:
                -- Block is committed when we're working on a later block
                -- (Working on block n+1 implies we commit block n)
                -- Nullable for joining nodes before genesis processed
                committed_block_hash    BLOB,
                -- Safety: track highest QC seen (highest view for ordered execution)
                -- Nullable for joining nodes before genesis processed
                highest_qc_block_hash   BLOB,
                highest_qc_phase        INTEGER CHECK(highest_qc_phase IN (0, 1)),  -- 0=propose, 1=lock

                FOREIGN KEY (last_propose_vote_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (prepared_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (committed_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (highest_qc_block_hash) REFERENCES blocks(block_hash)
            );

            CREATE TABLE metrics (
                from_node       INTEGER NOT NULL,
                to_node         INTEGER NOT NULL,
                start_time      TEXT NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      INTEGER,
                height          INTEGER NOT NULL,  -- Consensus height for deterministic versioning
                available       INTEGER NOT NULL DEFAULT 1, -- Node availability (0 if unreachable)
                storage_total_gb INTEGER,  -- Total storage capacity in GB
                storage_used_gb INTEGER,   -- Used storage capacity in GB

                PRIMARY KEY     (from_node, to_node, start_time),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node)   REFERENCES nodes(node_id)
            );

            -- Create indexes for common query patterns
            CREATE INDEX idx_metrics_time_range ON metrics(start_time, from_node, to_node);
            CREATE INDEX idx_metrics_from_node ON metrics(from_node, start_time);
            CREATE INDEX idx_metrics_to_node ON metrics(to_node, start_time);
            CREATE INDEX idx_metrics_height ON metrics(height DESC, to_node); -- For placement decisions at specific heights

            -- File system
            CREATE TABLE data_blocks (
                id               TEXT PRIMARY KEY,
                modified_at      TEXT,
                file_hash        BLOB NOT NULL,
                fragment_count   INTEGER NOT NULL,
                added_bytes      INTEGER NOT NULL,
                placement_height INTEGER,  -- Consensus height when fragment placement was determined
                file_size        INTEGER NOT NULL  -- Total size of the file in bytes (i64, max ~9.2 EB)
            );

            CREATE TABLE file_access (
                data_block_id    TEXT NOT NULL,
                user_id          INTEGER NOT NULL,
                ephemeral_pubkey BLOB NOT NULL,  -- 32 bytes X25519 ephemeral public key
                encrypted_file_key BLOB NOT NULL, -- 48 bytes (32 + 16 auth tag)

                PRIMARY KEY (data_block_id, user_id),
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE fragment_hashes (
                data_block_id    TEXT NOT NULL,
                chunk_number     INTEGER NOT NULL,
                local_index      INTEGER NOT NULL,
                fragment_id      TEXT NOT NULL,
                fragment_hash    BLOB NOT NULL,
                chunk_type       INTEGER NOT NULL CHECK(chunk_type IN (0, 1)),  -- 0=original, 1=recovery
                stored_locally   INTEGER DEFAULT 0,

                PRIMARY KEY (data_block_id, chunk_number, local_index),
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
            );

            -- Index for DHT lookups: which files contain fragment X
            CREATE INDEX idx_fragment_hash ON fragment_hashes(fragment_hash);

            CREATE TABLE inodes (
                -- stable identifier for FileProvider (UUIDv7 encodes creation time)
                id              TEXT UNIQUE NOT NULL,
                -- owner of this reference
                owner_id        INTEGER REFERENCES users(user_id) NOT NULL,
                -- denormalized deterministically encrypted string
                -- enables fast folder listing queries without need for recursive parent_id
                path            TEXT NOT NULL,
                -- type of the inode
                type            INTEGER NOT NULL CHECK(type IN (0, 1)),  -- 0=file, 1=folder
                -- FK to the content block
                data_id         TEXT REFERENCES data_blocks(id),

                PRIMARY KEY     (owner_id, path)
            );

            -- 1. The MOST IMPORTANT index for listing folder contents.
            -- Don't need text_pattern_ops due to ART index
            CREATE INDEX idx_inodes_path ON inodes (path);

            -- 2. An index to quickly find all inodes belonging to a specific user.
            CREATE INDEX idx_inodes_owner ON inodes (owner_id);
            
            -- 3. Index for FileProvider lookups by stable ID
            CREATE INDEX idx_inodes_id ON inodes (id);

            -- NOTE: modification_log is NOT consensus tracked - it's used for local FileProvider state delta computation
            -- This table tracks all file/folder modifications to support incremental sync in FileProvider
            -- It provides a unified change tracking mechanism for all file system operations
            CREATE TABLE modification_log (
                inode_id           TEXT NOT NULL,     -- Stable inode identifier
                owner_id           INTEGER NOT NULL,
                old_parent_id      TEXT,              -- Parent folder BEFORE modification (NULL for new items)
                modified_at_height INTEGER NOT NULL,

                PRIMARY KEY (inode_id, modified_at_height),
                FOREIGN KEY (owner_id) REFERENCES users(user_id)
            );

            -- Index for efficient queries: what was modified for user X since height Y?
            CREATE INDEX idx_modification_log_height ON modification_log (owner_id, modified_at_height);

            -- User data takeout tracking (consensus-tracked for network-wide coordination)
            CREATE TABLE takeouts (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(user_id),
                owner_node_id INTEGER NOT NULL,         -- Node that owns and processes this takeout
                status INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3, 4)),  -- 0=pending, 1=materializing, 2=ready, 3=expired, 4=cancelled
                expires_at TEXT NOT NULL,
                consensus_height INTEGER NOT NULL
            );

            -- Index for efficient lookups of active takeouts and cleanup
            CREATE INDEX idx_takeouts_user_status ON takeouts (user_id, status);
            CREATE INDEX idx_takeouts_expires ON takeouts (expires_at);
            CREATE INDEX idx_takeouts_owner_node ON takeouts (owner_node_id);

            -- Local staging table for fragment request metrics (before consensus submission)
            CREATE TABLE pending_fragment_requests (
                from_node INTEGER NOT NULL,
                to_node INTEGER NOT NULL,
                success INTEGER NOT NULL,
                recorded_at_height INTEGER NOT NULL,      -- When request actually occurred
                batch_upload_height INTEGER,              -- When submitted to consensus (NULL = pending)

                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node) REFERENCES nodes(node_id)
            );

            CREATE INDEX idx_pending_requests ON pending_fragment_requests (batch_upload_height, recorded_at_height);
            CREATE INDEX idx_timing_requests ON pending_fragment_requests (recorded_at_height, from_node, to_node);

            -- Consensus-tracked reputation metrics (aggregated from staging tables)
            CREATE TABLE fragment_request_metrics (
                reporting_node INTEGER NOT NULL,    -- Node that reported these metrics
                from_node INTEGER NOT NULL,         -- Node that requested fragments
                to_node INTEGER NOT NULL,           -- Node that served fragments
                consensus_height INTEGER NOT NULL,   -- When metrics were submitted
                requests_sent INTEGER NOT NULL,
                requests_succeeded INTEGER NOT NULL,

                PRIMARY KEY (reporting_node, from_node, to_node, consensus_height),
                FOREIGN KEY (reporting_node) REFERENCES nodes(node_id),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node) REFERENCES nodes(node_id)
            );

            -- Indexes for reputation queries
            CREATE INDEX idx_reputation_to_node ON fragment_request_metrics (to_node, consensus_height);
            CREATE INDEX idx_reputation_from_node ON fragment_request_metrics (from_node, consensus_height);
            CREATE INDEX idx_reputation_consensus_height ON fragment_request_metrics (consensus_height);

            CREATE TABLE fragment_inventory (
                fragment_hash           BLOB NOT NULL,
                node_id                 INTEGER NOT NULL,
                self_verified_height    INTEGER, -- Once every so often we ensure this verification is actual disk check NOT only DB check.

                PRIMARY KEY (fragment_hash, node_id),
                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            -- Indexes for fragment discovery optimization
            CREATE INDEX idx_fragment_inventory_node ON fragment_inventory (node_id, fragment_hash);  -- Node-specific fragment lookup
            CREATE INDEX idx_fragment_inventory_height ON fragment_inventory (self_verified_height, node_id);  -- Height-based queries

            -- Device tokens for OS integration authentication (consensus-replicated)
            -- API key format: {token_id}.{secret} - only hash of secret is stored
            -- Device name is SIV-encrypted with user's key (privacy from other nodes)
            -- Device tokens for OS integration authentication (consensus-replicated)
            -- API key format: {token_id}.{secret} - only hash of secret is stored
            -- Device name is SIV-encrypted with user's key (privacy from other nodes)
            CREATE TABLE device_tokens (
                id                      TEXT PRIMARY KEY,   -- UUIDv7 encodes creation time
                user_id                 INTEGER NOT NULL,
                api_key_hash            BLOB NOT NULL,      -- Blake3 hash of secret portion
                encrypted_device_name   TEXT NOT NULL,      -- SIV-encrypted, hex-encoded
                wrapped_user_key        BLOB NOT NULL,      -- ChaCha20-Poly1305 wrapped user privkey
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
            CREATE INDEX idx_device_tokens_user_id ON device_tokens(user_id);

            -- Sharing: pending share invitations
            CREATE TABLE incoming_shares (
                id                       TEXT PRIMARY KEY,
                data_block_id            TEXT NOT NULL,
                sender_id                INTEGER NOT NULL,
                recipient_id             INTEGER NOT NULL,
                file_access              BLOB NOT NULL,
                display_ephemeral_pubkey BLOB NOT NULL,
                encrypted_display_name   BLOB NOT NULL,
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
                FOREIGN KEY (sender_id) REFERENCES users(user_id),
                FOREIGN KEY (recipient_id) REFERENCES users(user_id)
            );
            CREATE INDEX idx_incoming_shares_recipient ON incoming_shares(recipient_id);
            CREATE INDEX idx_incoming_shares_data_block ON incoming_shares(data_block_id);

            -- Transaction nonce dedup (prevents stale resubmission after forward timeout)
            CREATE TABLE committed_tx_nonces (
                nonce TEXT PRIMARY KEY
            );

            -- Sharing: live-link membership
            CREATE TABLE shares (
                data_block_id   TEXT NOT NULL,
                user_id         INTEGER NOT NULL,
                PRIMARY KEY (data_block_id, user_id),
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );
        "
    )?;
    Ok(())
}
