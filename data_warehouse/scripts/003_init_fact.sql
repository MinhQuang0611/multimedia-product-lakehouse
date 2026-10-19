\set ON_ERROR_STOP on
\getenv dw_db DW_DBNAME
\getenv dw_user DW_USER
\connect :"dw_db"
SET ROLE :"dw_user";

CREATE TABLE IF NOT EXISTS hm.fact_sales (
    date_key     integer     NOT NULL,
    customer_key integer     NOT NULL,
    product_key  integer     NOT NULL,                 -- phiên bản SCD2 đúng tại ngày mua
    shop_key     smallint    NOT NULL,
    channel_key  smallint    NOT NULL,
    basket_id    varchar(16) NOT NULL,                 -- degenerate dimension (luật kết hợp)
    sales_qty    integer     NOT NULL CHECK (sales_qty > 0),
    unit_price   double precision NOT NULL CHECK (unit_price > 0),
    revenue      double precision NOT NULL
) PARTITION BY RANGE (date_key);

CREATE TABLE IF NOT EXISTS hm.fact_product_daily (
    date_key        integer  NOT NULL,
    product_key     integer  NOT NULL,
    shop_key        smallint NOT NULL,
    list_price      double precision,
    stock_qty       integer  NOT NULL CHECK (stock_qty >= 0),
    rating_avg      numeric(6, 4),
    review_count    integer  NOT NULL,
    daily_sales_qty integer  NOT NULL,
    daily_revenue   double precision NOT NULL,
    PRIMARY KEY (date_key, product_key, shop_key)
) PARTITION BY RANGE (date_key);

DO $$
DECLARE t text; y int;
BEGIN
    FOREACH t IN ARRAY ARRAY['fact_sales', 'fact_product_daily'] LOOP
        FOR y IN 2018..2021 LOOP
            EXECUTE format('CREATE TABLE IF NOT EXISTS hm.%I PARTITION OF hm.%I FOR VALUES FROM (%s) TO (%s)',
                           t || '_' || y, t, y * 10000 + 101, (y + 1) * 10000 + 101);
        END LOOP;
        EXECUTE format('CREATE TABLE IF NOT EXISTS hm.%I PARTITION OF hm.%I DEFAULT', t || '_default', t);
    END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS fact_sales_date_idx     ON hm.fact_sales (date_key);
CREATE INDEX IF NOT EXISTS fact_sales_product_idx  ON hm.fact_sales (product_key);
CREATE INDEX IF NOT EXISTS fact_sales_customer_idx ON hm.fact_sales (customer_key);
CREATE INDEX IF NOT EXISTS fact_product_daily_product_idx ON hm.fact_product_daily (product_key);

CREATE OR REPLACE PROCEDURE hm.set_fact_fks(enable boolean)
LANGUAGE plpgsql AS $$
DECLARE fk record;
BEGIN
    FOR fk IN SELECT * FROM (VALUES
        ('fact_sales',         'fact_sales_date_fk',     'date_key',     'dim_date'),
        ('fact_sales',         'fact_sales_customer_fk', 'customer_key', 'dim_customer'),
        ('fact_sales',         'fact_sales_product_fk',  'product_key',  'dim_product'),
        ('fact_sales',         'fact_sales_shop_fk',     'shop_key',     'dim_shop'),
        ('fact_sales',         'fact_sales_channel_fk',  'channel_key',  'dim_channel'),
        ('fact_product_daily', 'fact_pd_date_fk',        'date_key',     'dim_date'),
        ('fact_product_daily', 'fact_pd_product_fk',     'product_key',  'dim_product'),
        ('fact_product_daily', 'fact_pd_shop_fk',        'shop_key',     'dim_shop')
    ) v(tbl, name, col, ref) LOOP
        EXECUTE format('ALTER TABLE hm.%I DROP CONSTRAINT IF EXISTS %I', fk.tbl, fk.name);
        IF enable THEN
            EXECUTE format('ALTER TABLE hm.%I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES hm.%I',
                           fk.tbl, fk.name, fk.col, fk.ref);
        END IF;
    END LOOP;
END $$;

CALL hm.set_fact_fks(true);
