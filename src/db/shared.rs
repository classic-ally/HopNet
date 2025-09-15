use super::*;

pub fn initialize(db: PooledConnection<DuckdbConnectionManager>) -> Result<(), DuckdbError> {
    db.execute_batch(
        "
            CREATE TABLE sequences (
                name            TEXT PRIMARY KEY,
                next_id         INTEGER NOT NULL
            );

            CREATE TABLE users (
                user_id         INTEGER PRIMARY KEY,
                username        VARCHAR NOT NULL,
                password_hash   VARCHAR NOT NULL,
                pubkey          BLOB NOT NULL,
                x25519_pubkey   BLOB NOT NULL,  -- 32 bytes X25519 public key for file access

                CONSTRAINT unique_username UNIQUE (username)
            );

            CREATE TABLE nodes (
                node_id         INTEGER PRIMARY KEY,
                name            VARCHAR NOT NULL,
                ip_address      VARCHAR NOT NULL,
                port            INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
                owner           INTEGER NOT NULL,
                pubkey          BLOB NOT NULL,

                -- CONSTRAINT enables indexed lookup of these
                CONSTRAINT unique_endpoint UNIQUE (ip_address, port),

                FOREIGN KEY (owner) REFERENCES users(user_id)
            );

            -- Common query patterns: 
            -- 1. user owns what nodes?
            CREATE INDEX idx_nodes_owner ON nodes(owner);
            -- 2. what node is this IP? (enabled by CONSTRAINT)

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
                phase               ENUM('propose', 'lock') NOT NULL,
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
                highest_qc_phase        ENUM('propose', 'lock') NOT NULL,  -- QC's phase
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
                is_active           BOOLEAN NOT NULL,

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
                user_privkey            BLOB NOT NULL,

                -- Consensus mechanics
                -- View stored in case of leader change without block written
                -- Block height not stored -> always computable
                current_phase           ENUM('propose', 'lock') NOT NULL DEFAULT 'propose',
                current_view            INTEGER NOT NULL DEFAULT 0,
                -- Track last view where we issued a timeout vote to prevent conflicting votes
                last_timeout_vote_view  INTEGER DEFAULT 0,
                -- Block is prepared when it has a QC
                prepared_block_hash     BLOB,
                -- HotStuff-2 efficiency improvement:
                -- Block is committed when we're working on a later block
                -- (Working on block n+1 implies we commit block n)
                committed_block_hash    BLOB NOT NULL,
                -- Safety: track highest QC seen (highest view for ordered execution)
                highest_qc_block_hash   BLOB NOT NULL,

                FOREIGN KEY (prepared_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (committed_block_hash) REFERENCES blocks(block_hash),
                FOREIGN KEY (highest_qc_block_hash) REFERENCES blocks(block_hash),

                FOREIGN KEY (node_id) REFERENCES nodes(node_id)
            );

            CREATE TABLE metrics (
                from_node       INTEGER NOT NULL,
                to_node         INTEGER NOT NULL,
                start_time      TIMESTAMP NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      BIGINT,
                height          INTEGER NOT NULL,  -- Consensus height for deterministic versioning
                available       BOOLEAN NOT NULL DEFAULT TRUE, -- Node availability (false if unreachable)
                storage_total_gb UINTEGER,  -- Total storage capacity in GB
                storage_used_gb UINTEGER,   -- Used storage capacity in GB
                
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
                id               UUID PRIMARY KEY,
                modified_at      TIMESTAMP,
                file_hash        BLOB NOT NULL,
                fragment_count   INTEGER NOT NULL,
                added_bytes      UTINYINT NOT NULL,
                placement_height INTEGER,  -- Consensus height when fragment placement was determined
                file_size        UBIGINT NOT NULL  -- Total size of the file in bytes
            );

            CREATE TABLE file_access (
                data_block_id    UUID NOT NULL,
                user_id          INTEGER NOT NULL,
                ephemeral_pubkey BLOB NOT NULL,  -- 32 bytes X25519 ephemeral public key
                encrypted_file_key BLOB NOT NULL, -- 48 bytes (32 + 16 auth tag)
                
                PRIMARY KEY (data_block_id, user_id),
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            CREATE TABLE fragment_hashes (
                data_block_id    UUID NOT NULL,
                fragment_index   INTEGER NOT NULL,
                fragment_id      UUID NOT NULL,
                fragment_hash    BLOB NOT NULL,
                chunk_type       ENUM('original', 'recovery') NOT NULL,
                stored_locally   BOOLEAN DEFAULT FALSE,
                
                PRIMARY KEY (data_block_id, fragment_index),
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
            );

            -- Index for DHT lookups: which files contain fragment X
            CREATE INDEX idx_fragment_hash ON fragment_hashes(fragment_hash);

            CREATE TABLE inodes (
                -- stable identifier for FileProvider (UUIDv7 encodes creation time)
                id              UUID UNIQUE NOT NULL,
                -- owner of this reference
                owner_id        INTEGER REFERENCES users(user_id) NOT NULL,
                -- denormalized deterministically encrypted string
                -- enables fast folder listing queries without need for recursive parent_id
                path            VARCHAR NOT NULL,
                -- type of the inode
                type            ENUM('file', 'folder') NOT NULL,
                -- FK to the content block
                data_id         UUID REFERENCES data_blocks(id),

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
                inode_id           UUID NOT NULL,     -- Stable inode identifier
                owner_id           INTEGER NOT NULL,
                old_parent_id      UUID,              -- Parent folder BEFORE modification (NULL for new items)
                modified_at_height INTEGER NOT NULL,
                
                PRIMARY KEY (inode_id, modified_at_height),
                FOREIGN KEY (owner_id) REFERENCES users(user_id)
            );
            
            -- Index for efficient queries: what was modified for user X since height Y?
            CREATE INDEX idx_modification_log_height ON modification_log (owner_id, modified_at_height);

            -- Add comments for documentation
            COMMENT ON TABLE modification_log IS 'Local-only table for tracking all file modifications to support FileProvider incremental sync (NOT consensus tracked)';
            COMMENT ON TABLE metrics IS 'Network performance metrics between distributed system nodes';
            COMMENT ON COLUMN metrics.rtt_latency IS 'Round-trip time latency in milliseconds';
            COMMENT ON COLUMN metrics.rtt_variance IS 'RTT variance in milliseconds';
            COMMENT ON COLUMN metrics.rtt_jitter IS 'RTT jitter in milliseconds';
            COMMENT ON COLUMN metrics.throughput IS 'Network throughput in bytes per second';
            COMMENT ON COLUMN metrics.height IS 'Consensus height for deterministic versioning';
            COMMENT ON COLUMN metrics.available IS 'Node availability (false if unreachable during measurement)';
            COMMENT ON COLUMN metrics.storage_total_gb IS 'Total storage capacity in gigabytes';
            COMMENT ON COLUMN metrics.storage_used_gb IS 'Used storage capacity in gigabytes';
        "
    )?;
    Ok(())
}
