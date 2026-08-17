-- takeout baseline — ordinal 0001 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

CREATE TABLE takeouts (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id),
            owner_node_id INTEGER NOT NULL,         -- Node that owns and processes this takeout
            status INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3, 4)),  -- 0=pending, 1=materializing, 2=ready, 3=expired, 4=cancelled
            expires_at TEXT NOT NULL,
            consensus_height INTEGER NOT NULL
        );

CREATE INDEX idx_takeouts_user_status ON takeouts (user_id, status);

CREATE INDEX idx_takeouts_expires ON takeouts (expires_at);

CREATE INDEX idx_takeouts_owner_node ON takeouts (owner_node_id);

CREATE TABLE imports (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id),
            owner_node_id INTEGER NOT NULL,
            status INTEGER NOT NULL DEFAULT 0 CHECK(status IN (0, 1, 2, 3))
        );

CREATE INDEX idx_imports_user_status ON imports (user_id, status);

CREATE INDEX idx_imports_owner_node ON imports (owner_node_id);

