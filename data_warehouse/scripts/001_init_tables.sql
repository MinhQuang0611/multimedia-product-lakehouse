\set ON_ERROR_STOP on
\getenv dw_db DW_DBNAME
\getenv dw_user DW_USER
\getenv bi_user DW_BI_USER
\connect :"dw_db"
SET ROLE :"dw_user";

CREATE SCHEMA IF NOT EXISTS gold_hm;
CREATE SCHEMA IF NOT EXISTS hm;
CREATE SCHEMA IF NOT EXISTS etl_audit;

COMMENT ON SCHEMA gold_hm   IS 'Staging: bản sao nguyên trạng s3://lake/gold/hm (bảng phẳng đã làm sạch)';
COMMENT ON SCHEMA hm        IS 'Star schema H&M (6 dim + 2 fact) và view/materialized view cho BI';
COMMENT ON SCHEMA etl_audit IS 'Nhật ký nạp dữ liệu';

CREATE TABLE IF NOT EXISTS etl_audit.load_log (
    id          bigserial PRIMARY KEY,
    run_id      text,
    source_uri  text        NOT NULL,
    target      text        NOT NULL,
    rows        bigint      NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NOT NULL
);

GRANT USAGE ON SCHEMA hm, etl_audit TO :"bi_user";
GRANT SELECT ON ALL TABLES IN SCHEMA hm, etl_audit TO :"bi_user";
ALTER DEFAULT PRIVILEGES IN SCHEMA hm, etl_audit GRANT SELECT ON TABLES TO :"bi_user";

CREATE OR REPLACE FUNCTION hm.date_key(d date) RETURNS integer
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    RETURN (extract(year FROM d) * 10000 + extract(month FROM d) * 100 + extract(day FROM d))::integer;
