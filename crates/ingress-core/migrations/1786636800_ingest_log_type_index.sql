-- The status view's publish-timing window reads the newest `publish_pass`
-- events of the last hour (`WHERE event_type = ? AND at >= ? ORDER BY id
-- DESC LIMIT ?`). Without an index that scans the whole log, which grows
-- for 180 days; with it the read walks one type's newest rows backwards
-- and stops at the limit.
CREATE INDEX idx_ingest_log_type ON ingest_log(event_type, id);
