-- drive baseline — ordinal 0001 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

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

CREATE INDEX idx_inodes_path ON inodes (path);

CREATE INDEX idx_inodes_owner ON inodes (owner_id);

CREATE INDEX idx_inodes_id ON inodes (id);

CREATE TABLE modification_log (
            inode_id           TEXT NOT NULL,     -- Stable inode identifier
            owner_id           INTEGER NOT NULL,
            old_parent_id      TEXT,              -- Parent folder BEFORE modification (NULL for new items)
            modified_at_height INTEGER NOT NULL,

            PRIMARY KEY (inode_id, modified_at_height),
            FOREIGN KEY (owner_id) REFERENCES users(user_id)
        );

CREATE INDEX idx_modification_log_height ON modification_log (owner_id, modified_at_height);

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

CREATE TABLE shares (
            data_block_id   TEXT NOT NULL,
            user_id         INTEGER NOT NULL,
            PRIMARY KEY (data_block_id, user_id),
            FOREIGN KEY (data_block_id) REFERENCES data_blocks(id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

