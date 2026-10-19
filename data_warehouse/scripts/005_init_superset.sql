\set ON_ERROR_STOP on
\getenv dw_db DW_DBNAME
\getenv dw_user DW_USER
\connect :"dw_db"
SET ROLE :"dw_user";

CREATE OR REPLACE VIEW hm.v_sales AS
SELECT d.date, d.year_month, d.year_quarter, d.year, d.weekday, d.is_weekend,
       p.article_id, p.product_code, p.prod_name, p.product_type, p.product_group, p.garment_group,
       p.colour, p.appearance,
       k.department, k.section, k.index_name, k.index_group,
       s.shop_id, s.shop_name, s.node_id,
       c.customer_id, c.age, c.age_band, c.club_member_status, c.fashion_news_frequency, c.rfm_segment,
       ch.channel,
       f.basket_id, f.sales_qty, f.unit_price, f.revenue
FROM hm.fact_sales f
JOIN hm.dim_date d USING (date_key)
JOIN hm.dim_product p USING (product_key)          -- phiên bản SCD2 đúng tại ngày giao dịch
JOIN hm.dim_category k USING (category_key)
JOIN hm.dim_shop s USING (shop_key)
JOIN hm.dim_customer c USING (customer_key)
JOIN hm.dim_channel ch USING (channel_key);
COMMENT ON VIEW hm.v_sales IS 'fact_sales ⨝ mọi chiều — một dòng một dòng giao dịch';

CREATE OR REPLACE VIEW hm.v_product_daily AS
SELECT d.date, d.year_month, d.year_quarter, d.year,
       p.article_id, p.prod_name, p.product_type, p.product_group, k.department, k.index_group,
       s.shop_id, s.shop_name, s.node_id,
       f.list_price, f.stock_qty, f.rating_avg, f.review_count, f.daily_sales_qty, f.daily_revenue
FROM hm.fact_product_daily f
JOIN hm.dim_date d USING (date_key)
JOIN hm.dim_product p USING (product_key)
JOIN hm.dim_category k USING (category_key)
JOIN hm.dim_shop s USING (shop_key);
COMMENT ON VIEW hm.v_product_daily IS 'Snapshot article × shop × ngày; list_price/stock/rating KHÔNG cộng theo thời gian';

CREATE MATERIALIZED VIEW IF NOT EXISTS hm.mv_sales_daily AS
SELECT date, year_month, year_quarter, year, channel,
       sum(revenue) AS revenue, sum(sales_qty) AS units,
       count(DISTINCT basket_id) AS baskets, count(DISTINCT customer_id) AS customers
FROM hm.v_sales GROUP BY 1, 2, 3, 4, 5;

CREATE MATERIALIZED VIEW IF NOT EXISTS hm.mv_sales_month_group_channel AS
SELECT year, year_quarter, year_month, product_group, channel,
       sum(revenue) AS revenue, sum(sales_qty) AS units, count(DISTINCT basket_id) AS baskets
FROM hm.v_sales GROUP BY 1, 2, 3, 4, 5;

CREATE MATERIALIZED VIEW IF NOT EXISTS hm.mv_sales_quarter_index_age AS
SELECT year, year_quarter, index_group, age_band,
       sum(revenue) AS revenue, sum(sales_qty) AS units
FROM hm.v_sales GROUP BY 1, 2, 3, 4;

CREATE MATERIALIZED VIEW IF NOT EXISTS hm.mv_sales_month_shop AS
SELECT year, year_month, node_id, shop_id, shop_name,
       sum(revenue) AS revenue, sum(sales_qty) AS units, count(DISTINCT basket_id) AS baskets
FROM hm.v_sales GROUP BY 1, 2, 3, 4, 5;
