-- Rolling disk-truth sweep cursor (node-local, never in the snapshot):
-- where this node's walk resumes after a restart, so a node that keeps
-- restarting still completes a rotation. One row.
CREATE TABLE hopnet_storage_sweep_cursor (
    id                      INTEGER PRIMARY KEY CHECK (id = 1),
    next_shard              INTEGER NOT NULL CHECK (next_shard BETWEEN 0 AND 255),
    rotation                INTEGER NOT NULL DEFAULT 0,
    rotation_started_unix   INTEGER NOT NULL,
    rotation_started_height INTEGER NOT NULL
);
