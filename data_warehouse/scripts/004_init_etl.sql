\set ON_ERROR_STOP on
\getenv dw_db DW_DBNAME
\getenv dw_user DW_USER
\connect :"dw_db"
SET ROLE :"dw_user";

CREATE OR REPLACE PROCEDURE hm._log(p_run_id text, p_source text, p_target text, p_started timestamptz)
LANGUAGE plpgsql AS $$
DECLARE n bigint;
BEGIN
    EXECUTE format('SELECT count(*) FROM %s', p_target) INTO n;
    INSERT INTO etl_audit.load_log (run_id, source_uri, target, rows, started_at, finished_at)
    VALUES (p_run_id, p_source, p_target, n, p_started, clock_timestamp());
    RAISE NOTICE '%  ← %: % dòng (%)', rpad(p_target, 24), p_source, n, clock_timestamp() - p_started;
END $$;

CREATE OR REPLACE PROCEDURE hm.build(p_run_id text DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE
    t0 timestamptz;
    mv record;
    g_n bigint; g_rev double precision; f_n bigint; f_rev double precision;
    ok boolean;
BEGIN
    SET LOCAL work_mem = '256MB';
    CALL hm.set_fact_fks(false);
    TRUNCATE hm.fact_sales, hm.fact_product_daily, hm.dim_product, hm.dim_category,
             hm.dim_date, hm.dim_shop, hm.dim_customer, hm.dim_channel;

    t0 := clock_timestamp();
    INSERT INTO hm.dim_date
    SELECT hm.date_key(d::date), d::date, extract(day FROM d), extract(month FROM d), to_char(d, 'YYYY-MM'),
           extract(quarter FROM d), to_char(d, 'YYYY-"Q"Q'), extract(year FROM d),
           extract(isodow FROM d), extract(isodow FROM d) >= 6
    FROM generate_series((SELECT min(sale_date) FROM gold_hm.transaction),
                         (SELECT max(sale_date) FROM gold_hm.transaction), interval '1 day') d;
    CALL hm._log(p_run_id, 'gold_hm.transaction', 'hm.dim_date', t0);

    t0 := clock_timestamp();
    INSERT INTO hm.dim_category
    SELECT DISTINCT department_no, department, section_no, section, index_code, index_name,
           index_group_no, index_group
    FROM gold_hm.article;
    CALL hm._log(p_run_id, 'gold_hm.article', 'hm.dim_category', t0);

    t0 := clock_timestamp();
    INSERT INTO hm.dim_product
    WITH versions AS (
        SELECT a.article_id, coalesce(c.old_department_no, a.department_no) AS category_key,
               DATE '1900-01-01' AS valid_from,
               coalesce(c.change_date - 1, DATE '9999-12-31') AS valid_to
        FROM gold_hm.article a LEFT JOIN gold_hm.article_change c USING (article_id)
        UNION ALL
        SELECT article_id, new_department_no, change_date, DATE '9999-12-31'
        FROM gold_hm.article_change
    )
    SELECT row_number() OVER (ORDER BY v.article_id, v.valid_from), v.article_id, a.product_code,
           a.prod_name, a.product_type, a.product_group, a.garment_group, a.colour, a.appearance,
           v.category_key, v.valid_from, v.valid_to, v.valid_to = DATE '9999-12-31'
    FROM versions v JOIN gold_hm.article a USING (article_id);
    CALL hm._log(p_run_id, 'gold_hm.article + article_change', 'hm.dim_product', t0);

    t0 := clock_timestamp();
    INSERT INTO hm.dim_shop
    SELECT row_number() OVER (ORDER BY size_rank), shop_id, shop_name, size_rank, olist_weight, node_id
    FROM gold_hm.shop;
    CALL hm._log(p_run_id, 'gold_hm.shop', 'hm.dim_shop', t0);

    t0 := clock_timestamp();
    INSERT INTO hm.dim_customer
    SELECT row_number() OVER (ORDER BY customer_id), customer_id, age,
           CASE WHEN age IS NULL THEN 'Unknown' WHEN age < 25 THEN '16-24' WHEN age < 35 THEN '25-34'
                WHEN age < 45 THEN '35-44' WHEN age < 55 THEN '45-54' ELSE '55+' END,
           club_member_status, fashion_news_frequency, NULL
    FROM gold_hm.customer;
    CALL hm._log(p_run_id, 'gold_hm.customer', 'hm.dim_customer', t0);

    t0 := clock_timestamp();
    INSERT INTO hm.dim_channel
    SELECT channel_id, 'channel ' || channel_id FROM (SELECT DISTINCT channel_id FROM gold_hm.transaction) c;
    CALL hm._log(p_run_id, 'gold_hm.transaction', 'hm.dim_channel', t0);

    t0 := clock_timestamp();
    INSERT INTO hm.fact_sales
    SELECT hm.date_key(t.sale_date), c.customer_key, p.product_key, s.shop_key, t.channel_id,
           t.basket_id, t.sales_qty, t.unit_price, t.revenue
    FROM gold_hm.transaction t
    JOIN hm.dim_customer c USING (customer_id)
    JOIN hm.dim_product p ON p.article_id = t.article_id AND t.sale_date BETWEEN p.valid_from AND p.valid_to
    JOIN hm.dim_shop s USING (shop_id);
    CALL hm._log(p_run_id, 'gold_hm.transaction', 'hm.fact_sales', t0);

    t0 := clock_timestamp();
    CREATE TEMP TABLE _article_day ON COMMIT DROP AS
    WITH span AS (SELECT article_id, min(sale_date) AS lo, max(sale_date) AS hi
                  FROM gold_hm.transaction GROUP BY 1),
    price_day AS (SELECT article_id, sale_date AS date, sum(revenue) / sum(sales_qty) AS price
                  FROM gold_hm.transaction GROUP BY 1, 2),
    g AS (
        SELECT sp.article_id, d.date, d.date_key, pd.price,
               count(pd.price) OVER (PARTITION BY sp.article_id ORDER BY d.date) AS grp  -- nhóm "giá gần nhất"
        FROM span sp JOIN hm.dim_date d ON d.date BETWEEN sp.lo AND sp.hi
        LEFT JOIN price_day pd ON pd.article_id = sp.article_id AND pd.date = d.date
    )
    SELECT article_id, date, date_key, max(price) OVER (PARTITION BY article_id, grp) AS list_price
    FROM g;

    INSERT INTO hm.fact_product_daily
    WITH sold AS (
        SELECT article_id, shop_id, sale_date AS date, sum(sales_qty) AS qty, sum(revenue) AS rev
        FROM gold_hm.transaction GROUP BY 1, 2, 3
    ), rev_day AS (
        SELECT article_id, shop_id, review_date AS date, count(*) AS n, sum(score) AS s
        FROM gold_hm.review GROUP BY 1, 2, 3
    ), g AS (
        SELECT ad.article_id, ps.shop_id, ad.date, ad.date_key, ad.list_price,
               date_trunc('week', ad.date)::date AS week,
               coalesce(so.qty, 0) AS daily_qty, coalesce(so.rev, 0) AS daily_rev,
               coalesce(rd.n, 0) AS n_rev, coalesce(rd.s, 0) AS s_rev
        FROM _article_day ad
        JOIN gold_hm.article_shop ps ON ps.article_id = ad.article_id
        LEFT JOIN sold so ON so.article_id = ad.article_id AND so.shop_id = ps.shop_id AND so.date = ad.date
        LEFT JOIN rev_day rd ON rd.article_id = ad.article_id AND rd.shop_id = ps.shop_id AND rd.date = ad.date
    ), w AS (   -- mọi cửa sổ cùng thứ tự (article, shop, tuần, ngày) → Postgres chỉ sắp xếp một lần
        SELECT *,
               sum(n_rev) OVER cum AS review_count, sum(s_rev) OVER cum AS score_sum,
               sum(daily_qty) OVER (PARTITION BY article_id, shop_id, week ORDER BY date
                                    ROWS UNBOUNDED PRECEDING) AS wk_cum,
               sum(daily_qty) OVER (PARTITION BY article_id, shop_id, week) AS wk_total
        FROM g
        WINDOW cum AS (PARTITION BY article_id, shop_id ORDER BY week, date ROWS UNBOUNDED PRECEDING)
    ), s AS (
        SELECT *, ceil(1.2 * max(wk_total) OVER (PARTITION BY article_id, shop_id)) + 2 AS s_max FROM w
    )
    SELECT s.date_key, p.product_key, sh.shop_key, round(s.list_price::numeric, 6),
           (s.s_max - s.wk_cum)::integer,
           round(s.score_sum::numeric / nullif(s.review_count, 0), 4),
           s.review_count, s.daily_qty, s.daily_rev
    FROM s
    JOIN hm.dim_product p ON p.article_id = s.article_id AND s.date BETWEEN p.valid_from AND p.valid_to
    JOIN hm.dim_shop sh ON sh.shop_id = s.shop_id;
    CALL hm._log(p_run_id, 'gold_hm.transaction + article_shop + review', 'hm.fact_product_daily', t0);

    SELECT count(*), sum(revenue) INTO g_n, g_rev FROM gold_hm.transaction;
    SELECT count(*), sum(revenue) INTO f_n, f_rev FROM hm.fact_sales;
    IF g_n <> f_n OR abs(g_rev - f_rev) > 1e-6 * greatest(1, abs(g_rev)) THEN
        RAISE EXCEPTION 'fact_sales lệch gold_hm.transaction: % / % dòng, revenue % / %', f_n, g_n, f_rev, g_rev;
    END IF;
    SELECT (SELECT sum(daily_sales_qty) FROM hm.fact_product_daily) = (SELECT sum(sales_qty) FROM hm.fact_sales)
    INTO ok;
    IF NOT ok THEN
        RAISE EXCEPTION 'fact_product_daily: Σ daily_sales_qty ≠ Σ fact_sales.sales_qty';
    END IF;
    RAISE NOTICE 'đối chiếu: fact_sales % dòng, revenue % — KHỚP gold_hm.transaction', f_n, round(f_rev::numeric, 4);

    t0 := clock_timestamp();
    CALL hm.set_fact_fks(true);
    RAISE NOTICE 'gắn khoá ngoại fact → dim (%)', clock_timestamp() - t0;

    FOR mv IN SELECT matviewname FROM pg_matviews WHERE schemaname = 'hm' ORDER BY 1 LOOP
        t0 := clock_timestamp();
        EXECUTE format('REFRESH MATERIALIZED VIEW hm.%I', mv.matviewname);
        CALL hm._log(p_run_id, 'hm.fact_sales', 'hm.' || mv.matviewname, t0);
    END LOOP;

    ANALYZE hm.dim_date, hm.dim_category, hm.dim_product, hm.dim_shop, hm.dim_customer, hm.dim_channel,
            hm.fact_sales, hm.fact_product_daily;
END $$;
