-- Column profiles + catalog metadata (read by the AI data catalog + dashboard).
CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.column_profiles (
    table_name    TEXT             NOT NULL,
    column_name   TEXT             NOT NULL,
    data_type     TEXT             NOT NULL,
    row_count     BIGINT           NOT NULL,
    null_count    BIGINT           NOT NULL,
    null_rate     DOUBLE PRECISION NOT NULL,
    distinct_count BIGINT          NOT NULL,
    min_value     TEXT,
    max_value     TEXT,
    mean_value    DOUBLE PRECISION,
    sample_values TEXT[],
    description   TEXT,
    profiled_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name, column_name)
);
