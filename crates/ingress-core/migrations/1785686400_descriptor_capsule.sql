-- Publish-metadata capsule: the descriptor-derived fields publish needs
-- (media type, subtypes, favorite, capture metadata) persisted per photo at
-- materialization. Replaces the sidecar FILE as publish's metadata source —
-- state.db becomes the sole metadata store ahead of the archive→spool
-- transplant. NULL on pre-migration rows means "capsule not yet written";
-- publish skips such photos without burning attempts and the next
-- materialization or heal pass backfills the column.
ALTER TABLE photos ADD COLUMN descriptor_json TEXT;
