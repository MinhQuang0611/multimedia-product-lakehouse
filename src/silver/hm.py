from __future__ import annotations

from src.common.config import get_logger, load_config, paths
from src.common.lake import scan, write_table
from src.silver.quality import Rule, split_valid

log = get_logger("silver.hm")

ARTICLE_CATEGORICALS = [
    "prod_name", "product_type_name", "product_group_name", "graphical_appearance_name",
    "colour_group_name", "perceived_colour_value_name", "perceived_colour_master_name",
    "department_name", "index_code", "index_name", "index_group_name", "section_name",
    "garment_group_name",
]
ARTICLE_CODES = [
    "product_type_no", "graphical_appearance_no", "colour_group_code",
    "perceived_colour_value_id", "perceived_colour_master_id", "department_no",
    "index_group_no", "section_no", "garment_group_no",
]


def _unknown(col: str) -> str:
    return f"coalesce(nullif(trim({col}), ''), 'Unknown') AS {col}"


def build_article(con) -> dict:
    src = scan(paths().bronze / "hm" / "articles")
    cats = ",\n               ".join(_unknown(c) for c in ARTICLE_CATEGORICALS)
    codes = ",\n               ".join(f"try_cast({c} AS INTEGER) AS {c}" for c in ARTICLE_CODES)
    staged = f"""
        SELECT lpad(trim(article_id), 10, '0') AS article_id,
               lpad(trim(product_code), 7, '0') AS product_code,
               {cats},
               {codes},
               nullif(trim(detail_desc), '') AS detail_desc,
               _ingest_ts, _batch_id
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY lpad(trim(article_id), 10, '0')
                                   ORDER BY _ingest_ts DESC) = 1
    """
    rules = [
        Rule("A_ID_FORMAT", "NOT regexp_full_match(article_id, '[0-9]{10}')"),
        Rule("A_PRODUCT_CODE", "product_code IS NULL"),
    ]
    return split_valid(con, staged, rules, "hm/article")


def build_customer(con) -> dict:
    lo, hi = load_config()["quality"]["age_range"]
    src = scan(paths().bronze / "hm" / "customers")
    staged = f"""
        SELECT trim(customer_id) AS customer_id,
               coalesce(try_cast("FN" AS DOUBLE), 0) = 1     AS fn,
               coalesce(try_cast("Active" AS DOUBLE), 0) = 1 AS active,
               coalesce(upper(nullif(trim(club_member_status), '')), 'UNKNOWN') AS club_member_status,
               -- nguồn có cả 'NONE' lẫn 'None' → quy về một giá trị
               coalesce(upper(nullif(trim(fashion_news_frequency), '')), 'UNKNOWN') AS fashion_news_frequency,
               -- tuổi ngoài miền hợp lệ coi là thiếu, không loại khách hàng (giữ toàn vẹn tham chiếu)
               CASE WHEN try_cast(age AS INTEGER) BETWEEN {lo} AND {hi}
                    THEN try_cast(age AS INTEGER) END AS age,
               nullif(trim(postal_code), '') AS postal_code,
               _ingest_ts, _batch_id
        FROM {src}
        QUALIFY row_number() OVER (PARTITION BY trim(customer_id) ORDER BY _ingest_ts DESC) = 1
    """
    rules = [Rule("C_ID_FORMAT", "NOT regexp_full_match(customer_id, '[0-9a-f]{64}')")]
    return split_valid(con, staged, rules, "hm/customer")


def build_customer_sample(con) -> int:
    cfg = load_config()
    n, seed = cfg["sampling"]["n_customers"], cfg["seed"]
    src = scan(paths().bronze / "hm" / "transactions")
    q = f"""
        SELECT customer_id
        FROM (SELECT DISTINCT trim(customer_id) AS customer_id FROM {src})
        ORDER BY md5(customer_id || '{seed}')
        LIMIT {n}
    """
    rows = write_table(con, q, paths().silver / "hm" / "_sample_customers",
                       manifest={"n_customers": n, "seed": seed, "method": "md5(customer_id||seed)"})
    log.info("  mẫu khách hàng: %s", f"{rows:,}")
    return rows


def build_transaction(con) -> dict:
    s = paths().silver / "hm"
    con.execute(f"CREATE OR REPLACE TEMP TABLE _sample AS SELECT * FROM {scan(s / '_sample_customers')}")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _art AS SELECT article_id FROM {scan(s / 'article')}")
    con.execute(f"CREATE OR REPLACE TEMP TABLE _cus AS SELECT customer_id FROM {scan(s / 'customer')}")

    src = scan(paths().bronze / "hm" / "transactions")
    staged = f"""
        WITH t AS (
            SELECT try_cast(b.t_dat AS DATE)               AS t_dat,
                   trim(b.customer_id)                     AS customer_id,
                   lpad(trim(b.article_id), 10, '0')       AS article_id,
                   try_cast(b.price AS DOUBLE)             AS price,
                   try_cast(b.sales_channel_id AS SMALLINT) AS sales_channel_id
            FROM {src} b
            SEMI JOIN _sample s ON trim(b.customer_id) = s.customer_id
        )
        SELECT t_dat, customer_id, article_id, price, sales_channel_id,
               count(*)::INTEGER AS sales_qty,
               year(t_dat) AS year, month(t_dat) AS month
        FROM t
        GROUP BY ALL
    """
    min_price = load_config()["quality"]["min_price"]
    rules = [
        Rule("T_DATE", "t_dat IS NULL"),
        Rule("T_PRICE", f"price IS NULL OR price <= {min_price}"),
        Rule("T_CHANNEL", "sales_channel_id NOT IN (1, 2)"),
        Rule("T_FK_ARTICLE", "article_id NOT IN (SELECT article_id FROM _art)"),
        Rule("T_FK_CUSTOMER", "customer_id NOT IN (SELECT customer_id FROM _cus)"),
    ]
    return split_valid(con, staged, rules, "hm/transaction", partition_by=["year", "month"])


def run(con) -> None:
    log.info("Silver H&M")
    build_article(con)
    build_customer(con)
    build_customer_sample(con)
    build_transaction(con)
