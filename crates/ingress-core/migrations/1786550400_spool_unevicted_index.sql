-- The spool soft cap (spec §Storage-aware admission) reads the
-- materialized byte total on every claim: one file per content hash with
-- an unevicted row in any library, so `SUM(MAX(size_bytes)) GROUP BY
-- content_hash` over the unevicted rows. Covering and hash-ordered, the
-- GROUP BY streams off the index with no temp B-tree, and the evicted
-- majority of a long-running archive is never visited.
CREATE INDEX idx_blobs_unevicted ON blobs(content_hash, size_bytes)
    WHERE evicted_at IS NULL;
