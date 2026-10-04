-- This node's own uploads (node-local, never in the snapshot): a fragment
-- `api::put` wrote before the transaction carrying its fragment_hashes row
-- was signed. The only rowless file that matters is one of these, and
-- only the uploading node knows it, so the sweep never deletes a hash
-- listed here. A row is retired once its fragment_hashes row lands, or
-- purged by the operator for an upload that never committed.
CREATE TABLE hopnet_storage_local_uploads (
    fragment_hash   BLOB PRIMARY KEY,
    blob_id         TEXT NOT NULL,
    written_unix    INTEGER NOT NULL
);
CREATE INDEX idx_local_uploads_blob ON hopnet_storage_local_uploads (blob_id);
