-- storage step 0004 — scan support for the differential and the tick.
--
-- fragment_hashes.stored_locally is a node-local flag the sweep's
-- whole-node differential filters on both sides of two EXCEPT queries
-- (what this node holds vs. what consensus believes it holds). Without an
-- index each side walks the whole table; with ~2M rows that is most of a
-- minute per report. A partial index over the held rows keeps the
-- differential proportional to what the node holds.
CREATE INDEX idx_fragment_hashes_local ON fragment_hashes (fragment_hash) WHERE stored_locally = 1;
