from __future__ import annotations

import csv

from src.common.config import connect, get_logger, paths
from src.common.lake import scan
from src.silver import hm, olist, shopee, simulate
from src.silver.quality import missing_report

log = get_logger("silver")

# "sim" sau cùng: cần Silver H&M (giao dịch, article) và Silver Olist (phân phối tham chiếu).
BUILDERS = {"hm": hm.run, "olist": olist.run, "shopee": shopee.run, "sim": simulate.run}

PROFILED = [
    ("bronze", "hm/articles"), ("bronze", "hm/customers"), ("bronze", "hm/transactions"),
    ("bronze", "olist/order_reviews"), ("bronze", "shopee/train"),
    ("silver", "hm/article"), ("silver", "hm/customer"), ("silver", "hm/transaction"),
    ("silver", "olist/order"), ("silver", "olist/order_item"), ("silver", "olist/product"),
    ("silver", "olist/payment"), ("silver", "olist/review"), ("silver", "shopee/listing"),
]


def write_profile(con) -> None:
    p = paths()
    p.reports.mkdir(parents=True, exist_ok=True)

    rows = []
    for layer, table in PROFILED:
        path = (p.bronze if layer == "bronze" else p.silver) / table
        if path.exists():
            rows += missing_report(con, path, f"{layer}/{table}")
    with open(p.reports / "missing_values.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["table", "column", "rows", "missing_pct"])
        w.writerows(rows)

    write_source_summary(con)

    tx = p.silver / "hm" / "transaction"
    if not tx.exists():
        return
    s = con.execute(f"""
        SELECT min(t_dat), max(t_dat), count(*), sum(sales_qty),
               count(DISTINCT customer_id), count(DISTINCT article_id),
               count(DISTINCT (customer_id, t_dat))
        FROM {scan(tx)}
    """).fetchone()
    basket = con.execute(f"""
        SELECT avg(n), median(n), max(n) FROM (
            SELECT count(DISTINCT article_id) AS n FROM {scan(tx)} GROUP BY customer_id, t_dat)
    """).fetchone()
    by_month = con.execute(f"""
        SELECT year, month, count(*) FROM {scan(tx)} GROUP BY ALL ORDER BY ALL
    """).fetchall()

    md = [
        "# Profile dữ liệu Silver — H&M (mẫu)", "",
        f"- Khoảng thời gian: **{s[0]} → {s[1]}**",
        f"- Dòng giao dịch (sau gộp số lượng): **{s[2]:,}** — tổng đơn vị bán: {s[3]:,}",
        f"- Khách hàng: {s[4]:,} — sản phẩm có bán: {s[5]:,}",
        f"- Giỏ hàng (customer_id, t_dat): **{s[6]:,}**",
        f"- Số sản phẩm khác nhau / giỏ: TB {basket[0]:.2f}, trung vị {basket[1]:.0f}, max {basket[2]}",
        "", "## Số dòng theo tháng", "", "| Năm | Tháng | Dòng |", "|---|---|---|",
        *[f"| {y} | {m} | {n:,} |" for y, m, n in by_month],
        "", "Tỷ lệ thiếu theo cột: xem `missing_values.csv`. Nhật ký quarantine: `quality_log.csv`.",
    ]
    (p.reports / "profile_silver.md").write_text("\n".join(md), encoding="utf-8")
    log.info("Đã ghi báo cáo vào %s", p.reports)


def write_source_summary(con) -> None:
    import json

    p = paths()
    md = ["# Tóm tắt nguồn dữ liệu", "", "| Bảng | Bronze | Silver hợp lệ | Quarantine |", "|---|---|---|---|"]
    silver_of = {"hm/articles": "hm/article", "hm/customers": "hm/customer", "hm/transactions": "hm/transaction",
                 "olist/orders": "olist/order", "olist/order_items": "olist/order_item",
                 "olist/order_payments": "olist/payment", "olist/order_reviews": "olist/review",
                 "olist/customers": "olist/customer", "olist/sellers": "olist/seller",
                 "olist/products": "olist/product", "olist/category_translation": "olist/category",
                 "shopee/train": "shopee/listing"}
    for b, s in silver_of.items():
        mf = p.bronze / b / "_manifest.json"
        if not mf.exists():
            continue
        nb = json.loads(mf.read_text())["rows"]
        ns = con.execute(f"SELECT count(*) FROM {scan(p.silver / s)}").fetchone()[0] if (p.silver / s).exists() else ""
        nq = (con.execute(f"SELECT count(*) FROM {scan(p.quarantine / s)}").fetchone()[0]
              if (p.quarantine / s).has_parquet() else 0)
        md.append(f"| {b} | {nb:,} | {ns:,} | {nq:,} |")

    sp = p.silver / "shopee" / "listing"
    if sp.exists():
        t = scan(sp)
        n, imgs, grp = con.execute(f"SELECT count(*), count(DISTINCT image), count(DISTINCT label_group) "
                                   f"FROM {t}").fetchone()
        sizes = con.execute(f"""SELECT min(n), median(n), max(n), sum((n = 2)::INT), sum((n = 3)::INT)
                                FROM (SELECT count(*) n FROM {t} GROUP BY label_group)""").fetchone()
        md += ["", "## Shopee", "",
               f"- Listing: {n:,} — ảnh khác nhau: {imgs:,} — label_group: {grp:,}",
               f"- Listing dùng lại file ảnh của listing khác: {n - imgs:,}",
               f"- Cỡ nhóm: min {sizes[0]}, trung vị {sizes[1]:.0f}, max {sizes[2]}; "
               f"{sizes[3]:,} nhóm cỡ 2, {sizes[4]:,} nhóm cỡ 3"]
    (p.reports / "source_summary.md").write_text("\n".join(md), encoding="utf-8")


def run(only: list[str] | None = None) -> None:
    con = connect()
    for name, fn in BUILDERS.items():
        if not only or name in only:
            fn(con)
    write_profile(con)
