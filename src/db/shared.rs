use super::*;

use duckdb::{Connection, Error};

pub fn initialize() -> Result<Arc<Mutex<Connection>>, Error> {
    let db = Connection::open(":memory:")?;
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
                duration        SMALLINT NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      BIGINT,
                version         TINYINT NOT NULL DEFAULT 0,
                
                PRIMARY KEY     (from_node, to_node, start_time),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node)   REFERENCES nodes(node_id)
            );

            -- Create indexes for common query patterns
            CREATE INDEX idx_metrics_time_range ON metrics(start_time, from_node, to_node);
            CREATE INDEX idx_metrics_from_node ON metrics(from_node, start_time);
            CREATE INDEX idx_metrics_to_node ON metrics(to_node, start_time);

            -- File system
            CREATE TABLE data_blocks (
                id               UUID PRIMARY KEY,
                access_list      BLOB NOT NULL,
                modified_at      TIMESTAMP,
                file_hash        BLOB NOT NULL,
                fragment_count   INTEGER NOT NULL,
                added_bytes      UTINYINT NOT NULL
            );

            CREATE TABLE fragment_hashes (
                data_block_id    UUID NOT NULL,
                fragment_index   INTEGER NOT NULL,
                fragment_hash    BLOB NOT NULL,
                chunk_type       ENUM('original', 'recovery') NOT NULL,
                
                PRIMARY KEY (data_block_id, fragment_index),
                FOREIGN KEY (data_block_id) REFERENCES data_blocks(id)
            );

            -- Index for DHT lookups: which files contain fragment X
            CREATE INDEX idx_fragment_hash ON fragment_hashes(fragment_hash);

            CREATE TABLE inodes (
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


            -- Add comments for documentation
            COMMENT ON TABLE metrics IS 'Network performance metrics between distributed system nodes';
            COMMENT ON COLUMN metrics.duration IS 'Measurement duration in milliseconds (max ~32 seconds)';
            COMMENT ON COLUMN metrics.rtt_latency IS 'Round-trip time latency in milliseconds';
            COMMENT ON COLUMN metrics.rtt_variance IS 'RTT variance in milliseconds';
            COMMENT ON COLUMN metrics.rtt_jitter IS 'RTT jitter in milliseconds';
            COMMENT ON COLUMN metrics.throughput IS 'Network throughput in bytes per second';
            COMMENT ON COLUMN metrics.version IS 'Schema version for backwards compatibility';
        "
    )?;
    Ok(Arc::new(Mutex::new(db)))
}
