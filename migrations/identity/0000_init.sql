-- identity baseline — ordinal 0000 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

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
                onboarding_flags INTEGER NOT NULL DEFAULT 0, -- u32 bitfield, see hopnet_common::users::onboarding_flags

                CONSTRAINT unique_username UNIQUE (username)
            );

CREATE TABLE nodes (
                node_id         INTEGER PRIMARY KEY,
                name            TEXT NOT NULL,
                owner           INTEGER NOT NULL,
                pubkey          BLOB NOT NULL UNIQUE,
                -- Version attestation (RFC-019 S3): the node's objective
                -- self-claim, overwritten wholesale by each
                -- node_staged_version tx — a staged version upstream moved
                -- past vanishes at the next attestation. Codes are CalVer
                -- integers (src/version.rs); NULL until first attestation.
                -- staged stays NULL until a staging-capable upgrade
                -- provider exists (v1 git-release only reports). Read by
                -- the upgrade advisory now, the regenesis_start
                -- precondition later (S5).
                running_version_code    INTEGER,
                staged_version_code     INTEGER,
                version_attested_height INTEGER,

                FOREIGN KEY (owner) REFERENCES users(user_id)
            );

CREATE INDEX idx_nodes_owner ON nodes(owner);

CREATE TABLE this_node (
                internal_id             INTEGER PRIMARY KEY DEFAULT 1,
                node_id                 INTEGER NOT NULL UNIQUE,
                privkey                 BLOB NOT NULL,
                -- Node-local storage settings (RFC-STORAGE-002
                -- Configuration): per-node values touching nobody's
                -- determinism; surfaced in node settings UI later.
                hopnet_storage_gc_high_pct       INTEGER NOT NULL DEFAULT 90,
                hopnet_storage_gc_low_pct        INTEGER NOT NULL DEFAULT 80,
                hopnet_storage_reencode_enabled  INTEGER NOT NULL DEFAULT 1,
                hopnet_storage_repair_budget_pct INTEGER NOT NULL DEFAULT 10,
                -- Upgrade-provider settings (RFC-019 S3): node-local, no
                -- determinism impact. NULL release_url = derive the default
                -- from the crate's repository field.
                hopnet_upgrade_check_enabled     INTEGER NOT NULL DEFAULT 1,
                hopnet_upgrade_release_url       TEXT
            );

CREATE TABLE device_tokens (
                id                      TEXT PRIMARY KEY,   -- UUIDv7 encodes creation time
                user_id                 INTEGER NOT NULL,
                api_key_hash            BLOB NOT NULL,      -- Blake3 hash of secret portion
                encrypted_device_name   TEXT NOT NULL,      -- SIV-encrypted, hex-encoded
                wrapped_user_key        BLOB NOT NULL,      -- ChaCha20-Poly1305 wrapped user privkey
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

CREATE INDEX idx_device_tokens_user_id ON device_tokens(user_id);

