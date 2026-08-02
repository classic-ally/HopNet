-- Tombstone + restore propagation to the mesh (spec §Propagation to the
-- mesh; hand-sync with docs/specs/apple-photos-ingress.md).
--
-- `deleted_at` records what Apple Photos believes; `tombstone_published_at`
-- records what the mesh has been told. The delta is the queue:
--
--   deleted_at | tombstone_published_at | meaning
--   -----------+------------------------+---------------------------------
--   NULL       | NULL                   | live, mesh agrees — idle
--   set        | NULL                   | deleted locally, mesh not told
--   set        | set                    | deleted, mesh converged — idle
--   NULL       | set                    | restored locally, mesh still
--                                       |   tombstoned
--
-- Unlike `published_at`, this marker is deliberately RESETTABLE: delete →
-- restore → delete is a legitimate cycle a user can run any number of
-- times, and a restore clears it. If it stayed set, the second delete
-- would land in the converged state and never propagate, leaving the mesh
-- holding a photo Photos has discarded.
ALTER TABLE photos ADD COLUMN tombstone_published_at TEXT;

-- Its own retry ledger rather than reusing the publish trio. Publish
-- success resets those columns, so they are technically free — but a photo
-- that struggled to publish, succeeded, then failed to propagate its delete
-- would carry a blended failure history under a `publish_last_error`
-- string describing the wrong operation.
ALTER TABLE photos ADD COLUMN tombstone_publish_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE photos ADD COLUMN tombstone_publish_next_retry_at TEXT;
ALTER TABLE photos ADD COLUMN tombstone_publish_last_error TEXT;

-- Both queue directions are "published AND the two tombstone columns
-- disagree". Only published photos qualify: published_at IS NULL means the
-- mesh never heard of the photo, so there is nothing to tell it.
CREATE INDEX idx_photos_tombstone_pending ON photos(photo_id)
    WHERE published_at IS NOT NULL
      AND ((deleted_at IS NOT NULL AND tombstone_published_at IS NULL)
        OR (deleted_at IS NULL AND tombstone_published_at IS NOT NULL));
