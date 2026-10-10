-- Rolling sweep rotation history (node-local, never in the snapshot): each
-- completed rotation's height and time span. The attestation refresh
-- budgets its threshold for the longest recent rotation and dates stamps
-- against these points for its wall-clock cap.
CREATE TABLE hopnet_storage_sweep_rotations (
    rotation        INTEGER PRIMARY KEY,
    started_unix    INTEGER NOT NULL,
    started_height  INTEGER NOT NULL,
    ended_unix      INTEGER NOT NULL,
    ended_height    INTEGER NOT NULL
);
