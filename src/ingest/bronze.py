from __future__ import annotations

import json
from datetime import datetime, timezone

from src.common.config import connect, get_logger, load_config, new_batch_id, paths, sql_path
from src.common.lake import write_table
from src.crawl.tiki import SPLIT_KINDS

log = get_logger("ingest.bronze")

# (source, file csv, tên bảng bronze, phân vùng?)
TABLES = [
    ("hm", "articles.csv", "articles", False),
    ("hm", "customers.csv", "customers", False),
    ("hm", "transactions_train.csv", "transactions", True),
    ("olist", "olist_orders_dataset.csv", "orders", False),
    ("olist", "olist_order_items_dataset.csv", "order_items", False),
    ("olist", "olist_order_payments_dataset.csv", "order_payments", False),
    ("olist", "olist_order_reviews_dataset.csv", "order_reviews", False),
    ("olist", "olist_customers_dataset.csv", "customers", False),
    ("olist", "olist_sellers_dataset.csv", "sellers", False),
    ("olist", "olist_products_dataset.csv", "products", False),
    ("olist", "product_category_name_translation.csv", "category_translation", False),
    ("shopee", "train.csv", "train", False),
]


def load_table(con, source: str, csv_name: str, table: str, partitioned: bool, batch_id: str) -> int:
    src = paths().raw / source / csv_name
    if not src.exists():
        raise FileNotFoundError(f"{src} chưa có — chạy `python run.py download` trước")

    ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    select = f"""
        SELECT *,
               TIMESTAMPTZ '{ingest_ts}' AS _ingest_ts,
               '{batch_id}'              AS _batch_id,
               '{source}/{csv_name}'     AS _source_file
        FROM read_csv('{sql_path(src)}', header=true, all_varchar=true)
    """
    if partitioned:
        # t_dat dạng 'YYYY-MM-DD' — cắt chuỗi để phân vùng, không ép kiểu
        select = f"SELECT *, substr(t_dat,1,4) AS year, substr(t_dat,6,2) AS month FROM ({select})"

    final = paths().bronze / source / table
    mf = final / "_manifest.json"
    prev = json.loads(mf.read_text()) if mf.exists() else None
    n = write_table(
        con, select, final,
        partition_by=["year", "month"] if partitioned else None,
        manifest={"source_file": f"{source}/{csv_name}", "source_bytes": src.stat().st_size,
                  "batch_id": batch_id, "ingest_ts": ingest_ts},
    )
    log.info("  bronze/%-24s %12s dòng", f"{source}/{table}", f"{n:,}")
    check_batch_drift(f"{source}/{table}", prev, n, batch_id)
    return n


def check_batch_drift(table: str, prev: dict | None, n: int, batch_id: str) -> None:
    import csv

    if not prev:
        return
    limit = load_config()["quality"]["max_batch_drift"]
    drift = (n - prev["rows"]) / max(prev["rows"], 1)
    status = "OK" if abs(drift) <= limit else "DRIFT"
    if status != "OK":
        log.warning("  bronze/%s lệch %.1f%% so với batch %s (%s → %s dòng)",
                    table, 100 * drift, prev.get("batch_id"), f"{prev['rows']:,}", f"{n:,}")
    out = paths().reports / "batch_drift.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    new = not out.exists()
    with open(out, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["batch_id", "table", "prev_batch_id", "prev_rows", "rows", "drift_pct", "status"])
        w.writerow([batch_id, table, prev.get("batch_id"), prev["rows"], n, round(100 * drift, 3), status])


TIKI_KINDS = ["listing", "detail", "reviews", "sellers", "categories"]

# Các nguồn crawl khác: (nguồn, bảng bronze, mẫu file trong data/raw/<nguồn>/).
CRAWL_TABLES = [
    ("brands", "products", "*/*.jsonl.gz"),
    ("zara", "categories", "*/categories.jsonl.gz"),
    ("zara", "products", "*/products/**/*.jsonl.gz"),
    ("uniqlo", "listing", "*/listing/*.jsonl.gz"),
    ("uniqlo", "stock", "*/stock/*.jsonl.gz"),
    ("uniqlo", "details", "*/details/*.jsonl.gz"),
    ("uniqlo", "reviews", "*/reviews/*.jsonl.gz"),
]


def load_crawl(con, source: str, table: str, pattern: str, batch_id: str) -> int:
    # Crawl tích luỹ theo ngày: mỗi lần nạp lại toàn bộ snapshot, phân vùng theo ngày crawl.
    # Không kiểm tra batch drift vì bảng lớn dần theo số ngày đã crawl.
    root = paths().raw / source
    files = sorted(root.glob(pattern))
    if not files:
        return 0
    ingest_ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    select = f"""
        SELECT _crawl_ts, CAST(_req AS VARCHAR) AS _req, CAST(payload AS VARCHAR) AS payload,
               TIMESTAMPTZ '{ingest_ts}' AS _ingest_ts,
               '{batch_id}' AS _batch_id,
               regexp_extract(replace(filename, '\\', '/'), '({source}/.*)$', 1) AS _source_file,
               regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1) AS snapshot_date
        FROM read_json('{sql_path(root)}/{pattern}', format='newline_delimited', filename=true,
                       columns={{'_crawl_ts': 'VARCHAR', '_req': 'JSON', 'payload': 'JSON'}})
    """
    n = write_table(con, select, paths().bronze / source / table, partition_by=["snapshot_date"],
                    manifest={"source_file": f"{source}/{pattern}", "files": len(files),
                              "batch_id": batch_id, "ingest_ts": ingest_ts})
    days = len({f.relative_to(root).parts[0] for f in files})
    log.info("  bronze/%-24s %12s dòng (%d file, %d ngày)", f"{source}/{table}", f"{n:,}", len(files), days)
    return n


def load_tiki(con, kind: str, batch_id: str) -> int:
    pattern = f"*/{kind}/**/*.jsonl.gz" if kind in SPLIT_KINDS else f"*/{kind}.jsonl.gz"
    return load_crawl(con, "tiki", kind, pattern, batch_id)


def run(only: list[str] | None = None) -> None:
    con = connect()
    batch_id = new_batch_id()
    log.info("Bronze batch %s", batch_id)
    for source, csv_name, table, partitioned in TABLES:
        if only and source not in only:
            continue
        load_table(con, source, csv_name, table, partitioned, batch_id)
    if not only or "tiki" in only:
        for kind in TIKI_KINDS:
            load_tiki(con, kind, batch_id)
    for source, table, pattern in CRAWL_TABLES:
        if not only or source in only:
            load_crawl(con, source, table, pattern, batch_id)
