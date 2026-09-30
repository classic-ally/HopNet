-- storage step 0003 — RFC-STORAGE-003 S5: disk-truth attestation.
--
-- fragment_inventory gains the honest, REPLICATED verification record:
--   verified_height  the last height these bytes were seen on disk by a
--                    disk-verified attestation (attest_fragments); NULL
--                    until the first one. Confirm validation and read
--                    routing consume this, never the legacy column.
--   provenance       how the row was verified: 0 = this node's own disk
--                    scan; 1 = remote challenge (reserved for the
--                    proof-of-possession successor).
--   suspect          an inventory-row state that triggers repair exactly
--                    as a missing class does; self-scan never sets it —
--                    the successor's landing hook.
-- self_verified_height stays as the legacy, node-local (excluded) column,
-- written only at row insert now that the blanket restamp is gone.
ALTER TABLE fragment_inventory ADD COLUMN verified_height INTEGER;
ALTER TABLE fragment_inventory ADD COLUMN provenance INTEGER;
ALTER TABLE fragment_inventory ADD COLUMN suspect INTEGER NOT NULL DEFAULT 0;
CREATE INDEX idx_fragment_inventory_verified ON fragment_inventory (node_id, verified_height);
