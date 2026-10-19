from __future__ import annotations

from src.common.config import connect, get_logger, paths
from src.common.lake import scan, write_table

log = get_logger("olap.olist")

LEVELS = {"Thời gian": 4, "Khách hàng": 3, "Người bán": 3, "Sản phẩm": 2, "Thanh toán": 1}

CUBE_VIEW = """
    CREATE OR REPLACE TEMP VIEW sales_cube AS
    SELECT d.date, d.year_month, d.year_quarter, d.year,
           c.city AS c_city, c.state AS c_state, c.region AS c_region,
           s.city AS s_city, s.state AS s_state, s.region AS s_region,
           p.product_id, p.category,
           pm.payment_type,
           f.order_id, f.price, f.freight_value
    FROM fact_sales f
    JOIN dim_date d USING (date_key)
    JOIN dim_customer c USING (customer_key)
    JOIN dim_seller s USING (seller_key)
    JOIN dim_product p USING (product_key)
    LEFT JOIN dim_payment pm USING (payment_key)
    WHERE f.order_status NOT IN ('canceled', 'unavailable')
"""

QUERIES = [
    ("rollup_time", "Roll-up Thời gian: tháng → quý → năm → tổng (ROLLUP)", """
        SELECT year, year_quarter, year_month, round(sum(price)) AS revenue, count(DISTINCT order_id) AS orders
        FROM sales_cube WHERE year BETWEEN 2017 AND 2018
        GROUP BY ROLLUP (year, year_quarter, year_month)
        ORDER BY year NULLS LAST, year_quarter NULLS LAST, year_month NULLS LAST"""),
    ("rollup_geo", "Roll-up Khách hàng: bang → vùng", """
        SELECT c_region, round(sum(price)) AS revenue, count(DISTINCT order_id) AS orders,
               round(sum(price) / count(DISTINCT order_id), 1) AS avg_order_value,
               round(100 * sum(freight_value) / sum(price), 1) AS freight_pct
        FROM sales_cube GROUP BY c_region ORDER BY revenue DESC"""),
    ("drilldown", "Drill-down: 2018-Q2 → tháng → bang người bán (top 3 mỗi tháng)", """
        SELECT year_month, s_state, round(sum(price)) AS revenue
        FROM sales_cube WHERE year_quarter = '2018-Q2'
        GROUP BY year_month, s_state
        QUALIFY row_number() OVER (PARTITION BY year_month ORDER BY sum(price) DESC) <= 3
        ORDER BY year_month, revenue DESC"""),
    ("slice", "Slice: year = 2017 — doanh thu theo danh mục (top 10)", """
        SELECT category, round(sum(price)) AS revenue, count(*) AS items,
               round(avg(price), 1) AS avg_price
        FROM sales_cube WHERE year = 2017
        GROUP BY category ORDER BY revenue DESC LIMIT 10"""),
    ("dice", "Dice: quý ∈ {2018-Q1,2018-Q2} ∧ vùng KH ∈ {Sudeste,Sul} ∧ thanh toán ∈ {boleto,credit_card}", """
        SELECT year_quarter, c_region, payment_type, round(sum(price)) AS revenue,
               count(DISTINCT order_id) AS orders
        FROM sales_cube
        WHERE year_quarter IN ('2018-Q1', '2018-Q2') AND c_region IN ('Sudeste', 'Sul')
          AND payment_type IN ('boleto', 'credit_card')
        GROUP BY ALL ORDER BY ALL"""),
    ("pivot", "Pivot: Vùng KH × Quý (2018) — xoay quý thành cột", """
        PIVOT (SELECT c_region, year_quarter, price FROM sales_cube WHERE year = 2018)
        ON year_quarter USING round(sum(price)) GROUP BY c_region ORDER BY c_region"""),
    ("cube_region", "CUBE: Vùng KH × Vùng người bán — có đủ tổng dòng/cột/tổng chung", """
        SELECT coalesce(c_region, '(tất cả)') AS c_region, coalesce(s_region, '(tất cả)') AS s_region,
               round(sum(price)) AS revenue, grouping(c_region, s_region) AS grouping_id
        FROM sales_cube GROUP BY CUBE (c_region, s_region)
        ORDER BY grouping_id DESC, revenue DESC LIMIT 15"""),
]

MATERIALIZED = {
    "cuboid_month_category": ("(tháng, danh mục) — xu hướng danh mục theo thời gian, nguồn cho roll-up lên quý/năm",
        "SELECT year, year_quarter, year_month, category, sum(price) AS revenue, "
        "count(DISTINCT order_id) AS orders, count(*) AS items FROM sales_cube GROUP BY ALL"),
    "cuboid_quarter_cregion_sregion": ("(quý, vùng KH, vùng người bán) — phân tích luồng hàng liên vùng",
        "SELECT year, year_quarter, c_region, s_region, sum(price) AS revenue, "
        "sum(freight_value) AS freight, count(DISTINCT order_id) AS orders FROM sales_cube GROUP BY ALL"),
}


def _md_table(rel) -> str:
    cols = rel.columns
    rows = rel.fetchall()
    fmt = lambda v: "" if v is None else (f"{v:,.0f}" if isinstance(v, float) and abs(v) >= 1000 else str(v))
    return "\n".join(["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols),
                      *["| " + " | ".join(fmt(v) for v in r) + " |" for r in rows]])


def run() -> None:
    con = connect()
    g = paths().gold / "olist"
    for t in ["fact_sales", "dim_date", "dim_customer", "dim_seller", "dim_product", "dim_payment"]:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {t} AS SELECT * FROM {scan(g / t)}")
    con.execute(CUBE_VIEW)

    total = 1
    for L in LEVELS.values():
        total *= L + 1
    md = ["# OLAP — Olist", "",
          "Phân cấp: " + ", ".join(f"{k} {v} mức" for k, v in LEVELS.items()),
          f"Số khối con T = ∏(Lᵢ+1) = {' × '.join(str(v + 1) for v in LEVELS.values())} = **{total}**", ""]

    md += ["## Khối con vật chất hoá", ""]
    for name, (why, sql) in MATERIALIZED.items():
        n = write_table(con, sql, g / name)
        md.append(f"- `{name}` — {why} — {n:,} ô")
        log.info("  vật chất hoá %s: %s ô", name, f"{n:,}")
    md.append("")

    for _, title, sql in QUERIES:
        md += [f"## {title}", "", _md_table(con.sql(sql)), ""]
        log.info("  %s", title)

    out = paths().reports / "olist"
    out.mkdir(parents=True, exist_ok=True)
    (out / "olap.md").write_text("\n".join(md), encoding="utf-8")
    log.info("Đã ghi %s", out / "olap.md")
