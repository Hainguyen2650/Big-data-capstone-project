CREATE TABLE IF NOT EXISTS collector_runs (
    run_id              TEXT PRIMARY KEY,
    source              TEXT NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at         TIMESTAMPTZ,
    status              TEXT NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    records_seen        INTEGER NOT NULL DEFAULT 0,
    records_upserted    INTEGER NOT NULL DEFAULT 0,
    error_message       TEXT
);

CREATE INDEX IF NOT EXISTS idx_collector_runs_source_started
    ON collector_runs (source, started_at DESC);

CREATE TABLE IF NOT EXISTS financial_disclosures (
    disclosure_id       CHAR(64) PRIMARY KEY,
    source              TEXT NOT NULL CHECK (source IN ('hnx', 'ssc')),
    source_url          TEXT NOT NULL,
    ticker              TEXT,
    title               TEXT NOT NULL,
    report_type         TEXT,
    published_at        TIMESTAMPTZ,
    attachment_urls     JSONB NOT NULL DEFAULT '[]'::jsonb,
    local_files         JSONB NOT NULL DEFAULT '[]'::jsonb,
    raw_metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_run_id         TEXT REFERENCES collector_runs(run_id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE financial_disclosures
    ADD COLUMN IF NOT EXISTS local_files JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE financial_disclosures
    DROP CONSTRAINT IF EXISTS financial_disclosures_source_source_url_key;

CREATE INDEX IF NOT EXISTS idx_financial_disclosures_ticker_published
    ON financial_disclosures (ticker, published_at DESC);

CREATE INDEX IF NOT EXISTS idx_financial_disclosures_source_published
    ON financial_disclosures (source, published_at DESC);
