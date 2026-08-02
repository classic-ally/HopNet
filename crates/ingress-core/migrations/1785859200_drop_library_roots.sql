-- The archive is gone: blobs live in the process-global transient spool
-- under the data dir (<data_dir>/spool), and sidecar files no longer
-- exist, so libraries carry no storage paths. Library partitioning
-- survives in the ledger keys (blobs.library_id) and photo rows.
ALTER TABLE libraries DROP COLUMN blob_root;
ALTER TABLE libraries DROP COLUMN sidecar_root_remote;
ALTER TABLE photos DROP COLUMN sidecar_replicated_at;
