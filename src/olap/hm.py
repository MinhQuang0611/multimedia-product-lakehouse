from __future__ import annotations

from src.common.config import connect, get_logger, paths
from src.olap.olist import _md_table
from src.warehouse.load import dw_views

log = get_logger("olap.hm")

LEVELS = {"Thời gian": 4, "Sản phẩm": 3, "Khách hàng": 1, "Kênh": 1}

CUBE_VIEW = """
    CREATE OR REPLACE TEMP VIEW sales_cube AS
    SELECT d.date, d.year_month, d.year_quarter, d.year,
           p.article_id, p.product_type, p.product_group, k.department, k.section, k.index_group,
           s.shop_name, s.node_id, c.age_band, ch.channel, f.basket_id, f.sales_qty, f.revenue
    FROM fact_sales f
    JOIN dim_date d USING (date_key)
    JOIN dim_product p USING (product_key)          -- phiên bản SCD2 đúng tại ngày giao dịch
    JOIN dim_category k USING (category_key)
    JOIN dim_shop s USING (shop_key)
    JOIN dim_customer c USING (customer_key)
    JOIN dim_channel ch USING (channel_key)
"""

QUERIES = [
    ("rollup_time", "Roll-up Thời gian: tháng → quý → năm → tổng (ROLLUP)", """
        SELECT year, year_quarter, year_month, round(sum(revenue), 1) AS revenue,
               sum(sales_qty) AS units, count(DISTINCT basket_id) AS baskets
        FROM sales_cube GROUP BY ROLLUP (year, year_quarter, year_month)
        ORDER BY year NULLS LAST, year_quarter NULLS LAST, year_month NULLS LAST"""),
    ("rollup_product", "Roll-up Sản phẩm: product_type → product_group", """
        SELECT product_group, round(sum(revenue), 1) AS revenue, sum(sales_qty) AS units,
               round(100 * sum(revenue) / sum(sum(revenue)) OVER (), 1) AS revenue_pct,
               count(DISTINCT product_type) AS product_types
        FROM sales_cube GROUP BY product_group ORDER BY revenue DESC LIMIT 10"""),
    ("drilldown", "Drill-down: 2019-Q2 → tháng → product_type (top 3 mỗi tháng)", """
        SELECT year_month, product_type, sum(sales_qty) AS units
        FROM sales_cube WHERE year_quarter = '2019-Q2'
        GROUP BY year_month, product_type
        QUALIFY row_number() OVER (PARTITION BY year_month ORDER BY sum(sales_qty) DESC) <= 3
        ORDER BY year_month, units DESC"""),
    ("slice", "Slice: channel = 'channel 2' — theo index_group", """
        SELECT index_group, round(sum(revenue), 1) AS revenue, sum(sales_qty) AS units,
               round(100 * sum(revenue) / sum(sum(revenue)) OVER (), 1) AS revenue_pct
        FROM sales_cube WHERE channel = 'channel 2'
        GROUP BY index_group ORDER BY revenue DESC"""),
    ("dice", "Dice: quý ∈ {2019-Q2, 2020-Q2} ∧ index_group ∈ {Ladieswear, Divided} ∧ age_band ∈ {16-24, 25-34}", """
        SELECT year_quarter, index_group, age_band, round(sum(revenue), 1) AS revenue,
               sum(sales_qty) AS units
        FROM sales_cube
        WHERE year_quarter IN ('2019-Q2', '2020-Q2') AND index_group IN ('Ladieswear', 'Divided')
          AND age_band IN ('16-24', '25-34')
        GROUP BY ALL ORDER BY ALL"""),
    ("pivot", "Pivot: index_group × năm (revenue)", """
        PIVOT (SELECT index_group, year, revenue FROM sales_cube)
        ON year USING round(sum(revenue), 1) GROUP BY index_group ORDER BY index_group"""),
    ("cube_channel_age", "CUBE: Kênh × age_band — có tổng dòng/cột/tổng chung", """
        SELECT coalesce(channel, '(tất cả)') AS channel, coalesce(age_band, '(tất cả)') AS age_band,
               round(sum(revenue), 1) AS revenue, grouping(channel, age_band) AS grouping_id
        FROM sales_cube GROUP BY CUBE (channel, age_band)
        ORDER BY grouping_id DESC, channel, age_band"""),
]

MATERIALIZED = {
    "mv_sales_daily": "(ngày, kênh) — KPI chuỗi thời gian",
    "mv_sales_month_group_channel": "(tháng, product_group, kênh) — xu hướng nhóm hàng theo kênh",
    "mv_sales_quarter_index_age": "(quý, index_group, age_band) — phân khúc khách theo ngành hàng",
    "mv_sales_month_shop": "(tháng, shop, node) — doanh thu theo shop / node phân mảnh",
}


def run() -> None:
    con = connect()
    dw_views(con, "hm", ["fact_sales", "dim_date", "dim_product", "dim_category", "dim_shop", "dim_customer",
                         "dim_channel", *MATERIALIZED])
    con.execute(CUBE_VIEW.replace("TEMP VIEW", "TEMP TABLE"))

    total = 1
    for L in LEVELS.values():
        total *= L + 1
    md = ["# OLAP — H&M (mẫu 100.000 khách hàng)", "",
          "revenue = unit_price × sales_qty; giá H&M đã được chuẩn hoá, không phải đơn vị tiền tệ.", "",
          "Phân cấp: " + ", ".join(f"{k} {v} mức" for k, v in LEVELS.items()),
          f"Số khối con T = ∏(Lᵢ+1) = {' × '.join(str(v + 1) for v in LEVELS.values())} = **{total}**", "",
          "## Khối con vật chất hoá (materialized view trong DW, schema hm)", ""]
    for name, why in MATERIALIZED.items():
        n = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        md.append(f"- `hm.{name}` — {why} — {n:,} ô")
        log.info("  hm.%s: %s ô", name, f"{n:,}")
    md.append("")
    for _, title, sql in QUERIES:
        md += [f"## {title}", "", _md_table(con.sql(sql)), ""]
        log.info("  %s", title)

    out = paths().reports / "hm"
    out.mkdir(parents=True, exist_ok=True)
    (out / "olap.md").write_text("\n".join(md), encoding="utf-8")
    log.info("Đã ghi %s", out / "olap.md")
