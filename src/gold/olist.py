from __future__ import annotations

from src.common.config import connect, get_logger, paths
from src.common.lake import scan, write_table

log = get_logger("gold.olist")

REGION = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}


def _s(t: str) -> str:
    return scan(paths().silver / "olist" / t)


def _write(con, name: str, sql: str) -> None:
    n = write_table(con, sql, paths().gold / "olist" / name)
    log.info("  gold/olist/%-14s %10s dòng", name, f"{n:,}")


def _register_silver(con) -> None:
    for t in ["order", "order_item", "payment", "review", "customer", "seller", "product"]:
        con.execute(f'CREATE OR REPLACE TEMP VIEW s_{t} AS SELECT * FROM {_s(t)}')
    rows = ", ".join(f"('{st}', '{rg}')" for rg, sts in REGION.items() for st in sts)
    con.execute(f"CREATE OR REPLACE TEMP TABLE region_map(state, region) AS VALUES {rows}")


def build_dims(con) -> None:
    _write(con, "dim_date", """
        WITH bounds AS (SELECT min(purchase_ts)::DATE lo,
                               greatest(max(estimated_delivery_ts), max(delivered_customer_ts))::DATE hi
                        FROM s_order)
        SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
               d::DATE AS date, day(d) AS day, month(d) AS month,
               strftime(d, '%Y-%m') AS year_month,
               quarter(d) AS quarter, year(d) || '-Q' || quarter(d) AS year_quarter,
               year(d) AS year, isodow(d) AS weekday, isodow(d) >= 6 AS is_weekend
        FROM bounds, range(lo, hi + 1, INTERVAL 1 DAY) t(d)
    """)
    _write(con, "dim_customer", """
        WITH c AS (  -- một customer_unique_id có thể có nhiều customer_id (mỗi đơn một id);
                     -- lấy địa chỉ của đơn mới nhất làm địa chỉ hiện hành
            SELECT c.customer_unique_id, c.zip_prefix, c.city, c.state
            FROM s_customer c JOIN s_order o USING (customer_id)
            QUALIFY row_number() OVER (PARTITION BY c.customer_unique_id ORDER BY o.purchase_ts DESC) = 1
        )
        SELECT row_number() OVER (ORDER BY customer_unique_id) AS customer_key,
               c.*, coalesce(r.region, 'Unknown') AS region
        FROM c LEFT JOIN region_map r USING (state)
    """)
    _write(con, "dim_seller", """
        SELECT row_number() OVER (ORDER BY seller_id) AS seller_key,
               s.seller_id, s.zip_prefix, s.city, s.state, coalesce(r.region, 'Unknown') AS region
        FROM s_seller s LEFT JOIN region_map r USING (state)
    """)
    _write(con, "dim_product", """
        SELECT row_number() OVER (ORDER BY product_id) AS product_key,
               product_id, product_category_name AS category_pt, category_en AS category,
               photos_qty, name_length, description_length, weight_g,
               length_cm * height_cm * width_cm AS volume_cm3
        FROM s_product
    """)
    _write(con, "dim_payment", """
        WITH p AS (  -- loại thanh toán chính của đơn = loại có giá trị lớn nhất
            SELECT order_id, payment_type,
                   max(installments) OVER (PARTITION BY order_id) AS installments
            FROM s_payment
            QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY payment_value DESC) = 1
        ), combos AS (
            SELECT DISTINCT payment_type,
                   CASE WHEN installments <= 1 THEN '1' WHEN installments <= 3 THEN '2-3'
                        WHEN installments <= 6 THEN '4-6' ELSE '7+' END AS installment_band
            FROM p
        )
        SELECT row_number() OVER (ORDER BY payment_type, installment_band) AS payment_key, *
        FROM combos
    """)


def build_facts(con) -> None:
    g = paths().gold / "olist"
    for d in ["dim_customer", "dim_seller", "dim_product", "dim_payment"]:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {d} AS SELECT * FROM {scan(g / d)}")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE order_payment AS
        SELECT order_id, payment_type,
               CASE WHEN installments <= 1 THEN '1' WHEN installments <= 3 THEN '2-3'
                    WHEN installments <= 6 THEN '4-6' ELSE '7+' END AS installment_band,
               total_paid
        FROM (SELECT order_id, payment_type,
                     max(installments) OVER (PARTITION BY order_id) AS installments,
                     sum(payment_value) OVER (PARTITION BY order_id) AS total_paid
              FROM s_payment
              QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY payment_value DESC) = 1)
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE order_review AS   -- đơn có nhiều review → lấy review mới nhất
        SELECT order_id, review_score, comment_message IS NOT NULL AS has_comment, review_creation_ts
        FROM s_review
        QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY review_answer_ts DESC NULLS LAST) = 1
    """)

    _write(con, "fact_sales", """
        SELECT CAST(strftime(o.purchase_ts, '%Y%m%d') AS INTEGER) AS date_key,
               dc.customer_key, ds.seller_key, dp.product_key, pm.payment_key,
               i.order_id, i.order_item_id,            -- degenerate dimension
               o.order_status,
               1 AS quantity, i.price, i.freight_value, i.price + i.freight_value AS gross_value
        FROM s_order_item i
        JOIN s_order o USING (order_id)
        JOIN s_customer c USING (customer_id)
        JOIN dim_customer dc ON dc.customer_unique_id = c.customer_unique_id
        JOIN dim_seller ds USING (seller_id)
        JOIN dim_product dp USING (product_id)
        LEFT JOIN order_payment op USING (order_id)
        LEFT JOIN dim_payment pm ON pm.payment_type = op.payment_type
                                AND pm.installment_band = op.installment_band
    """)

    _write(con, "fact_order", """
        WITH items AS (
            SELECT order_id, count(*) AS n_items, count(DISTINCT product_id) AS n_products,
                   count(DISTINCT seller_id) AS n_sellers, sum(price) AS items_value,
                   sum(freight_value) AS freight_value, arg_max(seller_id, price) AS main_seller_id,
                   arg_max(product_id, price) AS main_product_id
            FROM s_order_item GROUP BY order_id
        )
        SELECT o.order_id,
               CAST(strftime(o.purchase_ts, '%Y%m%d') AS INTEGER)            AS purchase_date_key,
               CAST(strftime(o.approved_ts, '%Y%m%d') AS INTEGER)            AS approved_date_key,
               CAST(strftime(o.delivered_carrier_ts, '%Y%m%d') AS INTEGER)   AS carrier_date_key,
               CAST(strftime(o.delivered_customer_ts, '%Y%m%d') AS INTEGER)  AS delivered_date_key,
               CAST(strftime(o.estimated_delivery_ts, '%Y%m%d') AS INTEGER)  AS estimated_date_key,
               dc.customer_key, ds.seller_key AS main_seller_key, dp.product_key AS main_product_key,
               pm.payment_key, o.order_status,
               it.n_items, it.n_products, it.n_sellers, it.items_value, it.freight_value,
               op.total_paid,
               -- độ đo tích luỹ theo các mốc
               date_diff('hour', o.purchase_ts, o.approved_ts) / 24.0               AS approve_days,
               date_diff('hour', o.purchase_ts, o.delivered_carrier_ts) / 24.0      AS to_carrier_days,
               date_diff('hour', o.purchase_ts, o.delivered_customer_ts) / 24.0     AS delivery_days,
               date_diff('hour', o.purchase_ts, o.estimated_delivery_ts) / 24.0     AS promised_days,
               date_diff('hour', o.estimated_delivery_ts, o.delivered_customer_ts) / 24.0 AS delay_days,
               dc.state = ds.state AS same_state,
               rv.review_score, rv.has_comment
        FROM s_order o
        JOIN s_customer c USING (customer_id)
        JOIN dim_customer dc ON dc.customer_unique_id = c.customer_unique_id
        LEFT JOIN items it USING (order_id)
        LEFT JOIN dim_seller ds ON ds.seller_id = it.main_seller_id
        LEFT JOIN dim_product dp ON dp.product_id = it.main_product_id
        LEFT JOIN order_payment op USING (order_id)
        LEFT JOIN dim_payment pm ON pm.payment_type = op.payment_type
                                AND pm.installment_band = op.installment_band
        LEFT JOIN order_review rv USING (order_id)
    """)


def reconcile(con) -> None:
    g = paths().gold / "olist"
    silver_sum, silver_n = con.execute("SELECT sum(price), count(*) FROM s_order_item").fetchone()
    gold_sum, gold_n = con.execute(f"SELECT sum(price), count(*) FROM {scan(g / 'fact_sales')}").fetchone()
    ok = abs(silver_sum - gold_sum) < 1e-6 and silver_n == gold_n
    log.info("  đối chiếu fact_sales: silver %s dòng / %.2f — gold %s dòng / %.2f → %s",
             f"{silver_n:,}", silver_sum, f"{gold_n:,}", gold_sum, "KHỚP" if ok else "LỆCH")
    if not ok:
        raise AssertionError("fact_sales không khớp Silver")


def run() -> None:
    con = connect()
    log.info("Gold Olist")
    _register_silver(con)
    build_dims(con)
    build_facts(con)
    reconcile(con)
