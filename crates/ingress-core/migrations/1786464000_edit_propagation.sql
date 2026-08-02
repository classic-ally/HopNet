-- Re-edit and metadata propagation to the mesh (spec §Propagation of
-- edits; hand-sync with docs/specs/apple-photos-ingress.md).
--
-- Same vocabulary as tombstone propagation: a `published_*` column records
-- what the mesh holds, and its disagreement with the live value IS the
-- queue. Here the marker is value-keyed rather than existence-keyed —
-- deletion is a bit, but an edit is a version, and only the version can
-- say whether the mesh is current.
--
--   content_hash | published_content_hash | meaning
--   -------------+------------------------+--------------------------------
--   set          | equal                  | mesh has these bytes — idle
--   set          | different              | re-edited, mesh not told
--   set          | NULL                   | resource minted after publish
--                |                        |   (a first edit), never told
--   NULL         | (any)                  | mid-refetch; written_at is NULL
--                |                        |   and the row is not claimable
ALTER TABLE photo_resources ADD COLUMN published_content_hash TEXT;

-- A revert deletes the local row, and a deleted row takes its marker with
-- it — divergence-as-absence would be invisible to every predicate above.
-- So a row the mesh knows about is soft-deleted instead, and hard-deleted
-- once the removal propagates. The BLOB is still reaped immediately: the
-- payload names the kind, not the bytes.
ALTER TABLE photo_resources ADD COLUMN removed_at TEXT;

-- The metadata counterpart. `asset_modified_at` is what PhotoKit last
-- reported; this is the value the mesh's ciphertext was composed from.
-- Exactly as precise as the detector that drives it — classify only
-- rewrites the descriptor capsule when the modification date advances, so
-- a marker keyed on that date cannot miss a refresh it would have seen.
ALTER TABLE photos ADD COLUMN published_asset_modified_at TEXT;

-- Its own retry ledger, for the same reason tombstones got one: a photo
-- that struggled to publish, succeeded, then failed to propagate an edit
-- would otherwise carry a blended history under a `publish_last_error`
-- describing the wrong operation. The resource's own retry_count belongs
-- to the FETCH path and must not absorb a rejected transaction either.
ALTER TABLE photos ADD COLUMN edit_publish_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE photos ADD COLUMN edit_publish_next_retry_at TEXT;
ALTER TABLE photos ADD COLUMN edit_publish_last_error TEXT;

-- Backfill: an already-published photo IS current as far as anyone knows,
-- so its markers must read as converged from the first tick after upgrade.
-- Left NULL, every previously published photo would queue a full re-edit —
-- and fail it, because those blobs were evicted from the spool the moment
-- they were published. Same reasoning as the adoption stamp: "the mesh has
-- it" and "the mesh has never been told" must never be the same state.
UPDATE photos SET published_asset_modified_at = asset_modified_at
    WHERE published_at IS NOT NULL;

UPDATE photo_resources SET published_content_hash = content_hash
    WHERE written_at IS NOT NULL
      AND EXISTS (SELECT 1 FROM photos p
                  WHERE p.photo_id = photo_resources.photo_id
                    AND p.published_at IS NOT NULL);

-- Resource-side half of the claim predicate. A written row whose marker
-- disagrees with its hash, or a soft-removed row the mesh still holds.
CREATE INDEX idx_photo_resources_edit_pending ON photo_resources(photo_id)
    WHERE (removed_at IS NOT NULL AND published_content_hash IS NOT NULL)
       OR (removed_at IS NULL AND written_at IS NOT NULL
           AND published_content_hash IS NOT content_hash);

-- Metadata half. Both halves additionally require the photo to be
-- published, materialized and live — see `editable_photos`.
CREATE INDEX idx_photos_metadata_pending ON photos(photo_id)
    WHERE published_at IS NOT NULL
      AND deleted_at IS NULL
      AND published_asset_modified_at IS NOT asset_modified_at;
