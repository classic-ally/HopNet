-- Spool eviction stamp: the blob's local bytes were deleted after every
-- referencing photo was consensus-decided in HopNet (published or adopted).
-- The row survives with its refcount intact — refcounts mean "referencing
-- resource rows", not "bytes on disk", and Tier-1 repair recounts from
-- those rows. A later dedup hit on an evicted hash re-places the bytes and
-- clears the stamp.
ALTER TABLE blobs ADD COLUMN evicted_at TEXT;
