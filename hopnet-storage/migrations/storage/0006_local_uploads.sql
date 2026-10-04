-- This node's own uploads (node-local, never in the snapshot): a fragment
-- `api::put` is about to write before the transaction carrying its
-- fragment_hashes row is signed. The only rowless file that matters is one
-- of these, and only the uploading node knows it, so the sweep never
-- deletes a hash listed here. A row is retired once its fragment_hashes
-- row lands, expired once the retention passes without one, or purged by
-- the operator. Keyed per blob: two uploads sharing a fragment each hold
-- it, and a purge of one leaves the other's hold in place.
CREATE TABLE hopnet_storage_local_uploads (
    fragment_hash   BLOB NOT NULL,
    blob_id         TEXT NOT NULL,
    written_unix    INTEGER NOT NULL,
    PRIMARY KEY (fragment_hash, blob_id)
);
CREATE INDEX idx_local_uploads_blob ON hopnet_storage_local_uploads (blob_id);
