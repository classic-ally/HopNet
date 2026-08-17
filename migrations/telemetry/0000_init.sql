-- telemetry baseline — ordinal 0000 (RFC-020 S1).
-- Generated once from `initialize` by the baseliner tool, then
-- frozen: replay of this chain is the only installer. Never edit
-- a released step — append a new one.

CREATE TABLE metrics (
                from_node       INTEGER NOT NULL,
                to_node         INTEGER NOT NULL,
                start_time      TEXT NOT NULL,
                rtt_latency     REAL,
                rtt_variance    REAL,
                rtt_jitter      REAL,
                throughput      INTEGER,
                height          INTEGER NOT NULL,  -- Consensus height for deterministic versioning
                available       INTEGER NOT NULL DEFAULT 1, -- Node availability (0 if unreachable)
                storage_total_gb INTEGER,  -- Total storage capacity in GB
                storage_used_gb INTEGER,   -- Used storage capacity in GB

                PRIMARY KEY     (from_node, to_node, start_time),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node)   REFERENCES nodes(node_id)
            );

CREATE INDEX idx_metrics_time_range ON metrics(start_time, from_node, to_node);

CREATE INDEX idx_metrics_from_node ON metrics(from_node, start_time);

CREATE INDEX idx_metrics_to_node ON metrics(to_node, start_time);

CREATE INDEX idx_metrics_height ON metrics(height DESC, to_node);

CREATE TABLE pending_fragment_requests (
                from_node INTEGER NOT NULL,
                to_node INTEGER NOT NULL,
                success INTEGER NOT NULL,
                recorded_at_height INTEGER NOT NULL,      -- When request actually occurred
                batch_upload_height INTEGER,              -- When submitted to consensus (NULL = pending)

                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node) REFERENCES nodes(node_id)
            );

CREATE INDEX idx_pending_requests ON pending_fragment_requests (batch_upload_height, recorded_at_height);

CREATE INDEX idx_timing_requests ON pending_fragment_requests (recorded_at_height, from_node, to_node);

CREATE TABLE fragment_request_metrics (
                reporting_node INTEGER NOT NULL,    -- Node that reported these metrics
                from_node INTEGER NOT NULL,         -- Node that requested fragments
                to_node INTEGER NOT NULL,           -- Node that served fragments
                consensus_height INTEGER NOT NULL,   -- When metrics were submitted
                requests_sent INTEGER NOT NULL,
                requests_succeeded INTEGER NOT NULL,

                PRIMARY KEY (reporting_node, from_node, to_node, consensus_height),
                FOREIGN KEY (reporting_node) REFERENCES nodes(node_id),
                FOREIGN KEY (from_node) REFERENCES nodes(node_id),
                FOREIGN KEY (to_node) REFERENCES nodes(node_id)
            );

CREATE INDEX idx_reputation_to_node ON fragment_request_metrics (to_node, consensus_height);

CREATE INDEX idx_reputation_from_node ON fragment_request_metrics (from_node, consensus_height);

CREATE INDEX idx_reputation_consensus_height ON fragment_request_metrics (consensus_height);

