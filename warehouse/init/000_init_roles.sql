-- Create/update a dedicated read-only login for dashboard clients.
\getenv wh_app_password WH_APP_PASSWORD

DO $$
BEGIN
    IF current_user = 'wh_app' THEN
        RAISE EXCEPTION 'WH_ADMIN_USER must differ from the dashboard role wh_app';
    END IF;
END
$$;

SELECT 'CREATE ROLE wh_app WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wh_app')
\gexec

SELECT format('ALTER ROLE wh_app WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD %L', :'wh_app_password')
\gexec

ALTER ROLE wh_app SET search_path TO product_dashboard, sales_dashboard, customer_dashboard, marketing_dashboard, shared;
