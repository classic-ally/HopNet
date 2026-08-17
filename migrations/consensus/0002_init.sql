-- consensus baseline — ordinal 0002 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

CREATE TABLE committed_tx_nonces (
                nonce TEXT PRIMARY KEY
            );

CREATE TABLE regenesis_state (
                internal_id         INTEGER PRIMARY KEY CHECK (internal_id = 1),
                phase               INTEGER NOT NULL,  -- 1 moratorium | 2 sealed
                target_version_code INTEGER NOT NULL,  -- CalVer code the next epoch requires
                snapshot_hash       BLOB,              -- set by regenesis_commit
                seal_height         INTEGER            -- terminal H (bit-cast u64), set by regenesis_commit
            );

CREATE TABLE consensus_wal (
        height      INTEGER NOT NULL,
        seq         INTEGER NOT NULL,
        entry_type  INTEGER NOT NULL,
        entry       BLOB NOT NULL,
        PRIMARY KEY (height, seq)
    );

CREATE TABLE decided_blocks (
        height      INTEGER PRIMARY KEY,
        block_hash  BLOB NOT NULL UNIQUE,
        round       INTEGER NOT NULL,
        block       BLOB NOT NULL
    );

CREATE TABLE decided_certificates (
        height      INTEGER PRIMARY KEY,
        block_hash  BLOB NOT NULL,
        round       INTEGER NOT NULL,
        certificate BLOB NOT NULL
    );

CREATE TABLE consensus_meta (
        key         TEXT PRIMARY KEY,
        value       BLOB NOT NULL
    );

CREATE TABLE validators (
        effective_height    INTEGER NOT NULL,   -- height at which the state change takes effect
        node_id             INTEGER NOT NULL,
        is_active           INTEGER NOT NULL,
        -- Departure class (RFC-CONSENSUS-001 'Departure classes'):
        -- NULL on activation rows; lastDeparture = latest is_active=0 row.
        -- NULL-proofed: SQLite CHECKs pass on NULL, so the deactivation
        -- disjunct must assert IS NOT NULL explicitly.
        departure_kind      TEXT
            CHECK ((is_active = 1 AND departure_kind IS NULL)
                OR (is_active = 0 AND departure_kind IS NOT NULL
                    AND departure_kind IN ('voluntary', 'voted_out'))),
        PRIMARY KEY (effective_height, node_id)
    );

CREATE INDEX idx_validator_height ON validators(effective_height DESC);

CREATE INDEX idx_validator_active ON validators(effective_height, is_active);

CREATE INDEX idx_validator_node ON validators(node_id, effective_height DESC);

CREATE TABLE hopnet_consensus_policy (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );

