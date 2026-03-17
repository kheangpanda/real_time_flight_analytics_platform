-- ─────────────────────────────────────────────────────────────────────────────
-- Flight Analytics Platform — Data Warehouse Schema
-- ─────────────────────────────────────────────────────────────────────────────

-- ═════════════════════════════════════════════════════════════════════════════
-- RAW LAYER  —  exact copies of incoming Kafka messages
-- ═════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS raw.flight_events_raw (
    id              BIGSERIAL       PRIMARY KEY,
    event_id        UUID            NOT NULL DEFAULT uuid_generate_v4(),
    received_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    kafka_offset    BIGINT,
    kafka_partition INT,
    kafka_topic     VARCHAR(128),
    -- TEXT instead of JSONB: PySpark JDBC writes F.to_json() as VARCHAR.
    -- Ad-hoc JSON queries can still cast: payload::jsonb->'field'
    payload         TEXT            NOT NULL
);

-- Index: look up by event_id quickly
CREATE INDEX IF NOT EXISTS idx_raw_event_id
    ON raw.flight_events_raw (event_id);

-- Index: time-range scans (primary access pattern for raw layer)
CREATE INDEX IF NOT EXISTS idx_raw_received_at
    ON raw.flight_events_raw (received_at DESC);

-- Trigram index enables LIKE / regex searches on the raw JSON string
CREATE INDEX IF NOT EXISTS idx_raw_payload_trgm
    ON raw.flight_events_raw USING gin (payload gin_trgm_ops);

COMMENT ON TABLE raw.flight_events_raw IS
    'Landing zone. One row per Kafka message. Payload stored as TEXT to avoid '
    'JDBC type-mismatch; cast to JSONB at query time: payload::jsonb.';

-- ═════════════════════════════════════════════════════════════════════════════
-- STAGING LAYER  —  normalized, cleaned, typed records
-- ═════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS staging.flight_events_clean (
    id              BIGSERIAL       PRIMARY KEY,
    event_id        UUID            NOT NULL DEFAULT uuid_generate_v4(),
    raw_id          BIGINT          REFERENCES raw.flight_events_raw(id) ON DELETE SET NULL,

    -- Aircraft identity
    icao24          VARCHAR(16)     NOT NULL,
    callsign        VARCHAR(16),
    origin_country  VARCHAR(128),

    -- Position
    longitude       DOUBLE PRECISION,
    latitude        DOUBLE PRECISION,

    -- Altitude
    baro_altitude   DOUBLE PRECISION,   -- metres
    geo_altitude    DOUBLE PRECISION,   -- metres

    -- Kinematics
    velocity        DOUBLE PRECISION,   -- m/s
    velocity_kmh    DOUBLE PRECISION,   -- km/h  (derived)
    true_track      DOUBLE PRECISION,   -- degrees
    vertical_rate   DOUBLE PRECISION,   -- m/s

    -- Status
    on_ground       BOOLEAN,
    squawk          VARCHAR(8),
    spi             BOOLEAN,
    position_source INT,            -- LongType from Spark; INT avoids SMALLINT cast error

    -- Derived / classification
    altitude_level  VARCHAR(32),        -- 'ground','low','medium','high','very_high'
    flight_region   VARCHAR(64),        -- broad geographic region

    -- Time
    time_position   BIGINT,             -- unix epoch from source
    last_contact    BIGINT,             -- unix epoch from source
    ingested_at     TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    -- Quality flags
    is_valid        BOOLEAN             NOT NULL DEFAULT TRUE,
    validation_note TEXT
);

CREATE INDEX IF NOT EXISTS idx_staging_icao24
    ON staging.flight_events_clean (icao24);

CREATE INDEX IF NOT EXISTS idx_staging_country
    ON staging.flight_events_clean (origin_country);

CREATE INDEX IF NOT EXISTS idx_staging_ingested
    ON staging.flight_events_clean (ingested_at DESC);

CREATE INDEX IF NOT EXISTS idx_staging_on_ground
    ON staging.flight_events_clean (on_ground);

CREATE INDEX IF NOT EXISTS idx_staging_coords
    ON staging.flight_events_clean (longitude, latitude)
    WHERE longitude IS NOT NULL AND latitude IS NOT NULL;

COMMENT ON TABLE staging.flight_events_clean IS
    'Cleaned and enriched aircraft state vectors. One row per aircraft per poll cycle.';

-- ═════════════════════════════════════════════════════════════════════════════
-- MAIN LAYER  —  aggregated analytics tables
-- ═════════════════════════════════════════════════════════════════════════════

-- ─── Per-aircraft flight sessions ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS main.flight_analytics (
    id                  BIGSERIAL       PRIMARY KEY,
    icao24              VARCHAR(16)     NOT NULL,
    callsign            VARCHAR(16),
    origin_country      VARCHAR(128),

    -- Latest known position
    longitude           DOUBLE PRECISION,
    latitude            DOUBLE PRECISION,
    baro_altitude       DOUBLE PRECISION,

    -- Aggregated metrics (rolling window)
    avg_velocity_kmh    DOUBLE PRECISION,
    max_velocity_kmh    DOUBLE PRECISION,
    avg_altitude        DOUBLE PRECISION,
    max_altitude        DOUBLE PRECISION,
    avg_vertical_rate   DOUBLE PRECISION,

    -- Counts
    observation_count   INT             DEFAULT 1,
    on_ground_count     INT             DEFAULT 0,

    -- Classification labels
    altitude_level      VARCHAR(32),
    flight_region       VARCHAR(64),

    -- ML outputs
    predicted_velocity  DOUBLE PRECISION,
    anomaly_score       DOUBLE PRECISION,
    cluster_id          INT,

    -- Time window
    window_start        TIMESTAMPTZ,
    window_end          TIMESTAMPTZ,
    last_updated        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_main_icao24
    ON main.flight_analytics (icao24);

CREATE INDEX IF NOT EXISTS idx_main_country
    ON main.flight_analytics (origin_country);

CREATE INDEX IF NOT EXISTS idx_main_last_updated
    ON main.flight_analytics (last_updated DESC);

CREATE INDEX IF NOT EXISTS idx_main_window
    ON main.flight_analytics (window_start DESC, window_end DESC);

COMMENT ON TABLE main.flight_analytics IS
    'Aggregated per-aircraft metrics, refreshed by PySpark streaming micro-batches.';

-- ─── Country-level traffic summary ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS main.country_traffic_summary (
    id              BIGSERIAL       PRIMARY KEY,
    origin_country  VARCHAR(128)    NOT NULL,
    flight_count    INT             NOT NULL DEFAULT 0,
    avg_altitude    DOUBLE PRECISION,
    avg_velocity    DOUBLE PRECISION,
    snapshot_time   TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_country_summary_country
    ON main.country_traffic_summary (origin_country);

CREATE INDEX IF NOT EXISTS idx_country_summary_time
    ON main.country_traffic_summary (snapshot_time DESC);

COMMENT ON TABLE main.country_traffic_summary IS
    'Periodic country-level rollup for the bar-chart widget.';

-- ─── Hourly throughput time-series ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS main.hourly_flight_counts (
    id              BIGSERIAL       PRIMARY KEY,
    bucket          TIMESTAMPTZ     NOT NULL,   -- truncated to hour
    flight_count    INT             NOT NULL DEFAULT 0,
    avg_velocity    DOUBLE PRECISION,
    avg_altitude    DOUBLE PRECISION
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_hourly_bucket
    ON main.hourly_flight_counts (bucket);

COMMENT ON TABLE main.hourly_flight_counts IS
    'Hourly flight-count time series for the sparkline / time-series widget.';

-- ─── ML model registry ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS main.ml_model_registry (
    id              SERIAL          PRIMARY KEY,
    model_name      VARCHAR(128)    NOT NULL,
    model_type      VARCHAR(64),        -- 'regression','clustering','anomaly'
    version         VARCHAR(32),
    training_rows   INT,
    metric_name     VARCHAR(64),
    metric_value    DOUBLE PRECISION,
    artifact_path   TEXT,
    trained_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    is_active       BOOLEAN         NOT NULL DEFAULT FALSE
);

COMMENT ON TABLE main.ml_model_registry IS
    'Tracks trained model artifacts and their evaluation metrics.';
