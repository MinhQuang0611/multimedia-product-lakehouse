from __future__ import annotations

from src.common.config import connect, get_logger, paths
from src.common.lake import scan, write_table
from src.silver.simulate import shop_pick

log = get_logger("gold.hm")

TABLES = ["article", "article_change", "customer", "shop", "article_shop", "transaction", "review",
          "features_product", "baskets"]


def _write(con, name: str, sql: str, partition_by: list[str] | None = None) -> int:
    n = write_table(con, sql, paths().gold / "hm" / name, partition_by=partition_by)
    log.info("  gold/hm/%-16s %12s dòng", name, f"{n:,}")
    con.execute(f"CREATE OR REPLACE TEMP VIEW g_{name} AS SELECT * FROM {scan(paths().gold / 'hm' / name)}")
    return n


def _views(con) -> None:
    s, sim = paths().silver / "hm", paths().silver / "sim"
    for v, p in [("s_tx", s / "transaction"), ("s_article", s / "article"), ("s_customer", s / "customer"),
                 ("s_shop", sim / "shop"), ("s_ps", sim / "product_shop"), ("s_review", sim / "review"),
                 ("s_change", sim / "product_change")]:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {v} AS SELECT * FROM {scan(p)}")


def build_entities(con) -> None:
    _write(con, "article", """
        SELECT article_id, product_code, prod_name,
               product_type_no, product_type_name AS product_type, product_group_name AS product_group,
               garment_group_no, garment_group_name AS garment_group,
               colour_group_code, colour_group_name AS colour,
               perceived_colour_master_name AS colour_master,
               graphical_appearance_no, graphical_appearance_name AS appearance,
               department_no, department_name AS department, section_no, section_name AS section,
               index_code, index_name, index_group_no, index_group_name AS index_group,
               detail_desc
        FROM s_article ORDER BY article_id
    """)
    _write(con, "article_change", """
        SELECT article_id, change_date::DATE AS change_date,
               old_department_no::INTEGER AS old_department_no, new_department_no::INTEGER AS new_department_no
        FROM s_change ORDER BY article_id
    """)
    _write(con, "customer", """
        SELECT customer_id, age::SMALLINT AS age, club_member_status, fashion_news_frequency,
               fn AS is_fn, active AS is_active
        FROM s_customer SEMI JOIN (SELECT DISTINCT customer_id FROM s_tx) t USING (customer_id)
        ORDER BY customer_id
    """)
    _write(con, "shop", """
        SELECT shop_id, shop_name, size_rank::SMALLINT AS size_rank, weight AS olist_weight, node_id
        FROM s_shop ORDER BY size_rank
    """)
    _write(con, "article_shop", "SELECT article_id, shop_id FROM s_ps ORDER BY article_id, shop_id")


def build_transaction(con) -> None:
    _write(con, "transaction", f"""
        SELECT t.t_dat AS sale_date, t.customer_id, t.article_id, ps.shop_id,
               t.sales_channel_id AS channel_id,
               substr(md5(t.customer_id || '|' || t.t_dat), 1, 16) AS basket_id,
               t.sales_qty, t.price AS unit_price, t.price * t.sales_qty AS revenue,
               t.year, t.month
        FROM s_tx t JOIN s_ps ps ON ps.article_id = t.article_id AND {shop_pick()}
    """, partition_by=["year", "month"])
    _write(con, "review", """
        SELECT review_id, article_id, shop_id, customer_id,
               purchase_date::DATE AS purchase_date, review_date::DATE AS review_date,
               score::SMALLINT AS score
        FROM s_review ORDER BY review_id
    """)


def build_ml_tables(con) -> None:
    _write(con, "features_product", """
        WITH s AS (
            SELECT article_id, sum(sales_qty) AS units, sum(revenue) AS revenue,
                   count(DISTINCT basket_id) AS n_baskets, count(DISTINCT customer_id) AS n_customers,
                   sum(revenue) / sum(sales_qty) AS avg_price, max(unit_price) AS max_price,
                   min(sale_date) AS first_sale, max(sale_date) AS last_sale
            FROM g_transaction GROUP BY 1
        ), r AS (SELECT article_id, avg(score) AS rating_avg, count(*) AS review_count FROM g_review GROUP BY 1),
        ns AS (SELECT article_id, count(*) AS n_shops FROM g_article_shop GROUP BY 1)
        SELECT s.article_id, s.units, s.revenue, s.n_baskets, s.n_customers, s.avg_price, s.max_price,
               1 - s.avg_price / s.max_price AS discount,              -- mức giảm giá bình quân so với giá cao nhất
               date_diff('day', s.first_sale, s.last_sale) + 1 AS active_days,
               s.units / (date_diff('day', s.first_sale, s.last_sale) + 1) AS velocity,
               r.rating_avg, coalesce(r.review_count, 0) AS review_count,
               ns.n_shops, s.first_sale, s.last_sale
        FROM s LEFT JOIN r USING (article_id) JOIN ns USING (article_id)
    """)
    _write(con, "baskets", """
        SELECT basket_id, any_value(customer_id) AS customer_id, any_value(sale_date) AS sale_date,
               count(DISTINCT article_id) AS n_items, sum(sales_qty) AS n_units, sum(revenue) AS revenue,
               list(DISTINCT article_id ORDER BY article_id) AS articles
        FROM g_transaction GROUP BY basket_id
    """)


def drop_stale() -> None:
    root = paths().gold / "hm"
    for name in root.children():
        if name not in TABLES and not name.startswith("."):
            (root / name).remove()
            log.info("  xoá bảng cũ gold/hm/%s", name)


def reconcile(con) -> None:
    checks = {
        "transaction = silver (dòng, doanh thu)": """
            SELECT (SELECT count(*) FROM s_tx) = (SELECT count(*) FROM g_transaction)
               AND abs((SELECT sum(price * sales_qty) FROM s_tx) - (SELECT sum(revenue) FROM g_transaction)) < 1e-6""",
        "mọi giao dịch có article/customer/shop": """
            SELECT count(*) = 0 FROM g_transaction t
            WHERE t.article_id NOT IN (SELECT article_id FROM g_article)
               OR t.customer_id NOT IN (SELECT customer_id FROM g_customer)
               OR t.shop_id NOT IN (SELECT shop_id FROM g_shop)""",
        "article_change: department mới = snapshot hiện tại": """
            SELECT count(*) = 0 FROM g_article_change c JOIN g_article a USING (article_id)
            WHERE c.new_department_no <> a.department_no""",
        "baskets = số giỏ trong transaction": """
            SELECT (SELECT count(*) FROM baskets_check) = (SELECT count(*) FROM g_baskets)""",
    }
    con.execute("CREATE OR REPLACE TEMP VIEW baskets_check AS SELECT DISTINCT basket_id FROM g_transaction")
    bad = []
    for name, sql in checks.items():
        ok = con.execute(sql).fetchone()[0]
        log.info("  đối chiếu %-50s %s", name, "KHỚP" if ok else "LỆCH")
        if not ok:
            bad.append(name)
    if bad:
        raise AssertionError(f"Gold H&M không khớp: {bad}")


def run() -> None:
    con = connect()
    log.info("Gold H&M (bảng phẳng)")
    _views(con)
    build_entities(con)
    build_transaction(con)
    build_ml_tables(con)
    reconcile(con)
    drop_stale()
