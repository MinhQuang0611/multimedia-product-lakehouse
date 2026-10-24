-- Small run-control table shared by all warehouse subjects.
CREATE TABLE IF NOT EXISTS shared.etl_runs (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT etl_runs_finished_after_start CHECK (finished_at IS NULL OR finished_at >= started_at),
    CONSTRAINT etl_runs_status_finished CHECK ((status = 'running') = (finished_at IS NULL))
);

CREATE INDEX IF NOT EXISTS ix_etl_runs_pipeline_started
    ON shared.etl_runs (pipeline_name, started_at DESC);

-- Future tables in these schemas grant SELECT through default privileges set by init.
