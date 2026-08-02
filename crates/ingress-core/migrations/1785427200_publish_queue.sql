-- HopNet publish queue (spec §HopNet publish queue).
-- DDL mirrored in docs/specs/apple-photos-ingress.md §Local State Schema —
-- do not edit shapes here without updating the spec.
--
-- `published_at` is the terminal publish state and the future buffer-mode GC
-- predicate; the three publish_* columns are the per-photo retry ledger for
-- the daemon's publish tick (attempts are NOT consumed while the node is
-- unreachable — the tick parks instead).

ALTER TABLE photos ADD COLUMN published_at TEXT;
ALTER TABLE photos ADD COLUMN publish_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE photos ADD COLUMN publish_next_retry_at TEXT;
ALTER TABLE photos ADD COLUMN publish_last_error TEXT;

CREATE INDEX idx_photos_unpublished ON photos(photo_id)
    WHERE published_at IS NULL AND materialized_at IS NOT NULL;
