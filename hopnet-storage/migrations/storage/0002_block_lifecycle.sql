-- storage step 0002 — RFC-STORAGE-003 S1: the block lifecycle goal column
-- and the storage-view transition record.
--
-- desired_placement_height is the blob's declared goal (the model's
-- `target`): NOT NULL, stamped at insert with the inserting block's
-- height, moved only by declare_placement_target. placement_height stays
-- the confirmed epoch (NULL = never confirmed).
--
-- Backfill: placed blobs emerge quiescent (desired = placement_height).
-- Never-placed blobs get the sentinel 0, which sorts below every recorded
-- transition, so the first staleness pass declares the whole stranded
-- class forward. A step cannot read the current height: it must be
-- deterministic on every node (RFC-020 contract rule 2), and the epoch
-- crossing prunes consensus_meta before fast-forward runs.
ALTER TABLE data_blocks ADD COLUMN desired_placement_height INTEGER NOT NULL DEFAULT 0;
UPDATE data_blocks SET desired_placement_height = placement_height WHERE placement_height IS NOT NULL;

-- The two work-list predicates: staleness (desired < T) and in-flight
-- (placement_height IS NOT desired).
CREATE INDEX idx_data_blocks_desired ON data_blocks(desired_placement_height);
CREATE INDEX idx_data_blocks_inflight ON data_blocks(placement_height, desired_placement_height);

-- The transition record: every height at which the derived storage view
-- changed, with the canonical snapshot of the placement inputs (members,
-- weights, selection metrics) at that height. Written at block apply on
-- every node from replicated inputs by a pure derivation, so it is
-- replicated state: exported across epochs and divergence-checked.
-- declare/confirm validation read it and must agree on every node.
CREATE TABLE storage_view_transitions (
    height   INTEGER PRIMARY KEY,
    snapshot BLOB NOT NULL
);
