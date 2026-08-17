-- storage baseline — ordinal 0001 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

CREATE TABLE data_blocks (
            id               TEXT PRIMARY KEY,
            modified_at      TEXT,
            file_hash        BLOB NOT NULL,
            fragment_count   INTEGER NOT NULL,
            added_bytes      INTEGER NOT NULL,
            placement_height INTEGER,  -- Consensus height when fragment placement was determined
            file_size        INTEGER NOT NULL  -- Total size of the file in bytes (i64, max ~9.2 EB)
        );

CREATE TABLE blob_access (
            blob_id          TEXT NOT NULL,
            recipient_pubkey BLOB NOT NULL,  -- 32 bytes X25519 (user or mesh key)
            ephemeral_pubkey BLOB NOT NULL,  -- 32 bytes X25519 per-wrap ephemeral
            wrapped_key      BLOB NOT NULL,  -- 48 bytes (32 + 16 auth tag)

            PRIMARY KEY (blob_id, recipient_pubkey),
            FOREIGN KEY (blob_id) REFERENCES data_blocks(id)
        );

CREATE INDEX idx_blob_access_recipient ON blob_access(recipient_pubkey);

CREATE TABLE mesh_key (
            internal_id INTEGER PRIMARY KEY CHECK(internal_id = 1),
            pubkey      BLOB NOT NULL,   -- 32 bytes X25519
            key_version INTEGER NOT NULL DEFAULT 1
        );

CREATE TABLE mesh_key_access (
            recipient_pubkey BLOB PRIMARY KEY, -- member's X25519 pubkey
            ephemeral_pubkey BLOB NOT NULL,
            wrapped_privkey  BLOB NOT NULL     -- 48 bytes (32 + 16 tag)
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

CREATE INDEX idx_fragment_hash ON fragment_hashes(fragment_hash);

CREATE TABLE fragment_inventory (
            fragment_hash           BLOB NOT NULL,
            node_id                 INTEGER NOT NULL,
            self_verified_height    INTEGER, -- Once every so often we ensure this verification is actual disk check NOT only DB check.

            PRIMARY KEY (fragment_hash, node_id),
            FOREIGN KEY (node_id) REFERENCES nodes(node_id)
        );

CREATE INDEX idx_fragment_inventory_node ON fragment_inventory (node_id, fragment_hash);

CREATE INDEX idx_fragment_inventory_height ON fragment_inventory (self_verified_height, node_id);

CREATE TABLE hopnet_storage_policy (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

CREATE TABLE hopnet_storage_pins (
            blob_id   TEXT NOT NULL,
            owner     TEXT NOT NULL,
            pinned_at TEXT NOT NULL,  -- informational only
            PRIMARY KEY (blob_id, owner)
        );

CREATE INDEX idx_hopnet_storage_pins_blob ON hopnet_storage_pins(blob_id);

