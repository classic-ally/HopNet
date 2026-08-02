-- Thumbnail backfill (spec §photo_resources): photos ingested before the
-- daemon generated renditions never re-deliver descriptors (the scan probes
-- unchanged photos Done), so their thumbnail_small/medium (5/6) rows are
-- minted here and the photo re-enters the drain queue. Idempotent:
-- INSERT OR IGNORE rides the (photo_id, resource_type) primary key.
--
-- Scope: library-bound, non-tombstoned, PhotoKit-addressable photos only.
--  * library_id NULL (unmapped scope): adoption re-delivers a sentinel-
--    bearing descriptor, which mints 5/6 then — inserting here would
--    collide with adoption's plain INSERT.
--  * deleted_at set (tombstoned): restore arrives with a live descriptor;
--    plan_changes diffs the sentinels in as add_resources and clears
--    materialized_at — self-healing, no rows needed while dead.
--  * local_id NULL (recovered, not yet re-attached): nothing can fetch;
--    minting would only burn retry budget.
--
-- published_at is deliberately untouched: already-published photos
-- re-materialize with thumbnails but never re-publish (published_at is
-- terminal; content-update propagation is a future phase).

INSERT OR IGNORE INTO photo_resources (photo_id, resource_type)
SELECT photo_id, 5 FROM photos
WHERE library_id IS NOT NULL AND deleted_at IS NULL AND local_id IS NOT NULL;

INSERT OR IGNORE INTO photo_resources (photo_id, resource_type)
SELECT photo_id, 6 FROM photos
WHERE library_id IS NOT NULL AND deleted_at IS NULL AND local_id IS NOT NULL;

-- Drain eligibility: pending_photos selects materialized_at IS NULL.
UPDATE photos SET materialized_at = NULL
WHERE library_id IS NOT NULL AND deleted_at IS NULL
  AND EXISTS (SELECT 1 FROM photo_resources r
              WHERE r.photo_id = photos.photo_id AND r.written_at IS NULL);
