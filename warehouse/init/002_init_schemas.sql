-- One schema per dashboard/subject area. Keep these names stable and lowercase.
CREATE SCHEMA IF NOT EXISTS product_dashboard AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS sales_dashboard AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS customer_dashboard AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS marketing_dashboard AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS shared AUTHORIZATION CURRENT_USER;

REVOKE CREATE ON SCHEMA public FROM PUBLIC;

SELECT format('GRANT CONNECT ON DATABASE %I TO wh_app', current_database())
\gexec
GRANT USAGE ON SCHEMA product_dashboard, sales_dashboard, customer_dashboard, marketing_dashboard, shared TO wh_app;
GRANT SELECT ON ALL TABLES IN SCHEMA product_dashboard, sales_dashboard, customer_dashboard, marketing_dashboard, shared TO wh_app;

-- Tables created later by the warehouse owner are readable by dashboard users.
ALTER DEFAULT PRIVILEGES IN SCHEMA product_dashboard, sales_dashboard, customer_dashboard, marketing_dashboard, shared
    GRANT SELECT ON TABLES TO wh_app;

-- Shared operational metadata, separate from dashboard marts.
CREATE TABLE IF NOT EXISTS shared.warehouse_meta (
    key text PRIMARY KEY,
    value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO shared.warehouse_meta (key, value)
VALUES ('initialized', 'true'), ('warehouse_version', '1')
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, updated_at = now();
