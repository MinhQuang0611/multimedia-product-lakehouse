from __future__ import annotations

from src.common.config import get_logger, paths
from src.common.lake import scan
from src.silver.quality import Rule, split_valid

log = get_logger("silver.olist")


def _b(table: str) -> str:
    return scan(paths().bronze / "olist" / table)


def _keys(con, name: str, table: str, col: str) -> None:
    con.execute(f"CREATE OR REPLACE TEMP TABLE {name} AS "
                f"SELECT {col} FROM {scan(paths().silver / 'olist' / table)}")


def build_category(con) -> dict:
    staged = f"""
        SELECT lower(trim(product_category_name)) AS product_category_name,
               lower(trim(product_category_name_english)) AS category_en,
               _ingest_ts, _batch_id
        FROM {_b('category_translation')}
        QUALIFY row_number() OVER (PARTITION BY lower(trim(product_category_name)) ORDER BY _ingest_ts DESC) = 1
    """
    return split_valid(con, staged, [Rule("CAT_NAME", "product_category_name IS NULL")], "olist/category")


def build_product(con) -> dict:
    _keys(con, "_cat", "category", "product_category_name, category_en")
    staged = f"""
        SELECT trim(p.product_id) AS product_id,
               coalesce(nullif(lower(trim(p.product_category_name)), ''), 'unknown') AS product_category_name,
               coalesce(c.category_en, 'unknown') AS category_en,
               try_cast(p.product_name_lenght AS INTEGER)        AS name_length,
               try_cast(p.product_description_lenght AS INTEGER) AS description_length,
               try_cast(p.product_photos_qty AS INTEGER) AS photos_qty,
               try_cast(p.product_weight_g AS DOUBLE)    AS weight_g,
               try_cast(p.product_length_cm AS DOUBLE)   AS length_cm,
               try_cast(p.product_height_cm AS DOUBLE)   AS height_cm,
               try_cast(p.product_width_cm AS DOUBLE)    AS width_cm,
               p._ingest_ts, p._batch_id
        FROM {_b('products')} p
        LEFT JOIN _cat c ON lower(trim(p.product_category_name)) = c.product_category_name
        QUALIFY row_number() OVER (PARTITION BY trim(p.product_id) ORDER BY p._ingest_ts DESC) = 1
    """
    rules = [Rule("P_ID", "product_id IS NULL OR length(product_id) <> 32")]
    return split_valid(con, staged, rules, "olist/product")


def build_customer(con) -> dict:
    staged = f"""
        SELECT trim(customer_id) AS customer_id,
               trim(customer_unique_id) AS customer_unique_id,
               lpad(trim(customer_zip_code_prefix), 5, '0') AS zip_prefix,
               lower(trim(customer_city))  AS city,
               upper(trim(customer_state)) AS state,
               _ingest_ts, _batch_id
        FROM {_b('customers')}
        QUALIFY row_number() OVER (PARTITION BY trim(customer_id) ORDER BY _ingest_ts DESC) = 1
    """
    rules = [Rule("CU_ID", "customer_id IS NULL OR customer_unique_id IS NULL"),
             Rule("CU_STATE", "NOT regexp_full_match(state, '[A-Z]{2}')")]
    return split_valid(con, staged, rules, "olist/customer")


def build_seller(con) -> dict:
    staged = f"""
        SELECT trim(seller_id) AS seller_id,
               lpad(trim(seller_zip_code_prefix), 5, '0') AS zip_prefix,
               lower(trim(seller_city))  AS city,
               upper(trim(seller_state)) AS state,
               _ingest_ts, _batch_id
        FROM {_b('sellers')}
        QUALIFY row_number() OVER (PARTITION BY trim(seller_id) ORDER BY _ingest_ts DESC) = 1
    """
    return split_valid(con, staged, [Rule("S_ID", "seller_id IS NULL OR seller_id = ''")], "olist/seller")


def build_order(con) -> dict:
    _keys(con, "_cus", "customer", "customer_id")
    staged = f"""
        SELECT trim(order_id)    AS order_id,
               trim(customer_id) AS customer_id,
               lower(trim(order_status)) AS order_status,
               try_cast(order_purchase_timestamp AS TIMESTAMP)      AS purchase_ts,
               try_cast(order_approved_at AS TIMESTAMP)             AS approved_ts,
               try_cast(order_delivered_carrier_date AS TIMESTAMP)  AS delivered_carrier_ts,
               try_cast(order_delivered_customer_date AS TIMESTAMP) AS delivered_customer_ts,
               try_cast(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_ts,
               _ingest_ts, _batch_id
        FROM {_b('orders')}
        QUALIFY row_number() OVER (PARTITION BY trim(order_id) ORDER BY _ingest_ts DESC) = 1
    """
    rules = [
        Rule("O_PURCHASE_TS", "purchase_ts IS NULL"),
        Rule("O_FK_CUSTOMER", "customer_id NOT IN (SELECT customer_id FROM _cus)"),
        Rule("O_DELIVERED_TS", "order_status = 'delivered' AND (delivered_customer_ts IS NULL "
                               "OR delivered_customer_ts < purchase_ts)"),
    ]
    return split_valid(con, staged, rules, "olist/order")


def build_order_item(con) -> dict:
    _keys(con, "_sel", "seller", "seller_id")
    _keys(con, "_prd", "product", "product_id")
    _keys(con, "_ord", "order", "order_id")
    staged = f"""
        SELECT trim(order_id) AS order_id,
               try_cast(order_item_id AS INTEGER) AS order_item_id,
               trim(product_id) AS product_id,
               trim(seller_id)  AS seller_id,
               try_cast(shipping_limit_date AS TIMESTAMP) AS shipping_limit_ts,
               try_cast(price AS DOUBLE)         AS price,
               try_cast(freight_value AS DOUBLE) AS freight_value,
               _ingest_ts, _batch_id
        FROM {_b('order_items')}
        QUALIFY row_number() OVER (PARTITION BY trim(order_id), try_cast(order_item_id AS INTEGER)
                                   ORDER BY _ingest_ts DESC) = 1
    """
    rules = [
        Rule("OI_PRICE", "price IS NULL OR price <= 0"),
        Rule("OI_FREIGHT", "freight_value IS NULL OR freight_value < 0"),
        Rule("OI_FK_SELLER", "seller_id NOT IN (SELECT seller_id FROM _sel)"),
        Rule("OI_FK_PRODUCT", "product_id NOT IN (SELECT product_id FROM _prd)"),
        Rule("OI_FK_ORDER", "order_id NOT IN (SELECT order_id FROM _ord)"),
    ]
    return split_valid(con, staged, rules, "olist/order_item")


def build_payment(con) -> dict:
    _keys(con, "_ord", "order", "order_id")
    staged = f"""
        SELECT trim(order_id) AS order_id,
               try_cast(payment_sequential AS INTEGER)   AS payment_sequential,
               lower(trim(payment_type))                 AS payment_type,
               try_cast(payment_installments AS INTEGER) AS installments,
               try_cast(payment_value AS DOUBLE)         AS payment_value,
               _ingest_ts, _batch_id
        FROM {_b('order_payments')}
    """
    rules = [
        Rule("PAY_VALUE", "payment_value IS NULL OR payment_value < 0"),
        Rule("PAY_TYPE", "payment_type NOT IN ('credit_card','boleto','voucher','debit_card')"),
        Rule("PAY_FK_ORDER", "order_id NOT IN (SELECT order_id FROM _ord)"),
    ]
    return split_valid(con, staged, rules, "olist/payment")


def build_review(con) -> dict:
    _keys(con, "_ord", "order", "order_id")
    # review_id lặp (một review gắn nhiều đơn): giữ bản trả lời mới nhất cho mỗi (review_id, order_id).
    staged = f"""
        SELECT trim(review_id) AS review_id,
               trim(order_id)  AS order_id,
               try_cast(review_score AS INTEGER) AS review_score,
               nullif(trim(review_comment_title), '')   AS comment_title,
               nullif(trim(review_comment_message), '') AS comment_message,
               try_cast(review_creation_date AS TIMESTAMP)    AS review_creation_ts,
               try_cast(review_answer_timestamp AS TIMESTAMP) AS review_answer_ts,
               _ingest_ts, _batch_id
        FROM {_b('order_reviews')}
        QUALIFY row_number() OVER (PARTITION BY trim(review_id), trim(order_id)
                                   ORDER BY try_cast(review_answer_timestamp AS TIMESTAMP) DESC NULLS LAST) = 1
    """
    rules = [Rule("R_SCORE", "review_score NOT BETWEEN 1 AND 5"),
             Rule("R_FK_ORDER", "order_id NOT IN (SELECT order_id FROM _ord)")]
    return split_valid(con, staged, rules, "olist/review")


def run(con) -> None:
    log.info("Silver Olist")
    build_category(con)
    build_product(con)
    build_customer(con)
    build_seller(con)
    build_order(con)
    build_order_item(con)
    build_payment(con)
    build_review(con)
