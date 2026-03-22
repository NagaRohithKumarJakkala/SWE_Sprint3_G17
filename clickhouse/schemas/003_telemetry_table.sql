-- ═══════════════════════════════════════════════════════════
-- 003: Main Telemetry MergeTree Storage Table
-- REQ-4.1: Replicated storage engine for data durability
-- REQ-4.2: Sorted and partitioned by service, timestamp, severity
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS telemetry.logs
(
    -- Core fields (REQ-1.9 schema)
    timestamp         DateTime64(9, 'UTC')  CODEC(DoubleDelta, ZSTD(1)),
    trace_id          String                CODEC(ZSTD(1)),
    span_id           String                CODEC(ZSTD(1)),
    service_name      LowCardinality(String),
    severity          LowCardinality(String),
    body              String                CODEC(ZSTD(3)),

    -- ML features (stored as JSON string, parsed on read)
    ml_features       String                CODEC(ZSTD(1)),

    -- Routing metadata
    is_anomalous      UInt8,
    anomaly_reason    LowCardinality(String),

    -- Aggregator metadata
    aggregator_received_at  DateTime64(9, 'UTC'),

    -- Insertion metadata
    inserted_at       DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMMDD(timestamp), service_name)
ORDER BY (service_name, severity, timestamp, trace_id)
TTL toDateTime(timestamp) + INTERVAL 7 DAY   -- REQ-RET-1: 7-day hot retention
SETTINGS
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400,
    storage_policy = 'default';
