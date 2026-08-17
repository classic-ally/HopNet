-- identity step 0001 — schema_ordinals (RFC-020 S3).
-- The node-local ordinal stamp: one row per module recording which
-- chain position this database file is at. The fast-forward's
-- validation input; written by install/fast-forward, validated at boot.
-- The chain's first real step: frozen once released (contract rule 1).

CREATE TABLE schema_ordinals (
    module          TEXT PRIMARY KEY,   -- == snapshot section name
    ordinal         INTEGER NOT NULL    -- a real position of that module's chain
);
