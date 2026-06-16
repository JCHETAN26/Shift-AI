-- Validation results store (read by the dashboard's Data Quality view).
-- Created in the source Postgres for convenience; logically a metadata table.
CREATE TABLE IF NOT EXISTS validation_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT        NOT NULL,
    suite         TEXT        NOT NULL,
    table_name    TEXT        NOT NULL,
    expectation   TEXT        NOT NULL,
    severity      TEXT        NOT NULL,   -- critical | warning
    success       BOOLEAN     NOT NULL,
    observed      JSONB,                  -- observed metrics (counts, etc.)
    detail        TEXT,
    validated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_validation_results_run ON validation_results(run_id);
CREATE INDEX IF NOT EXISTS idx_validation_results_suite ON validation_results(suite, validated_at DESC);
