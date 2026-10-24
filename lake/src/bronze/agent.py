import gzip
import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from src.common.core import LEGACY_SOURCES, RAW, Agent, Context, Task, layer, log, now_iso, read_json, \
    swap_dir, write_json

BRONZE_SCHEMA = pa.schema([
    ("_source", pa.string()), ("_dataset", pa.string()), ("_snapshot_date", pa.date32()),
    ("_crawl_ts", pa.string()), ("_file", pa.string()), ("_req", pa.string()), ("payload", pa.string()),
])
BATCH = 256
DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# CSV tĩnh của dataset Kaggle: (nguồn, file, dataset bronze)
LEGACY_TABLES = {
    "hm": [("articles.csv", "articles"), ("customers.csv", "customers"), ("transactions_train.csv", "transactions")],
    "shopee": [("train.csv", "train")],
    "olist": [("olist_products_dataset.csv", "products"), ("product_category_name_translation.csv", "category_translation"),
              ("olist_orders_dataset.csv", "orders"), ("olist_order_items_dataset.csv", "order_items"),
              ("olist_order_payments_dataset.csv", "payments"), ("olist_order_reviews_dataset.csv", "reviews"),
              ("olist_customers_dataset.csv", "customers"), ("olist_sellers_dataset.csv", "sellers"),
              ("olist_geolocation_dataset.csv", "geolocation")],
}


def read_lines(path: Path):
    # Mỗi dòng là một gzip member riêng; file đang được crawler append có thể cụt ở cuối → dừng êm.
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    return
    except (EOFError, gzip.BadGzipFile, OSError):
        return


def dataset_of(source: str, rel: Path) -> str:
    # brands: mỗi site một file nhưng cùng một dataset; còn lại dataset là thành phần đầu của đường dẫn.
    if source == "brands":
        return "products"
    return rel.parts[0].removesuffix(".jsonl.gz")


def raw_days(source: str) -> list[str]:
    root = RAW / source
    return sorted(p.name for p in root.iterdir() if p.is_dir() and DAY.match(p.name)) if root.is_dir() else []


def raw_datasets(source: str, day: str) -> dict[str, list[Path]]:
    base = RAW / source / day
    out: dict[str, list[Path]] = {}
    for p in sorted(base.rglob("*.jsonl.gz")):
        out.setdefault(dataset_of(source, p.relative_to(base)), []).append(p)
    return out


def signature(files: list[Path]) -> str:
    h = hashlib.sha1()
    for p in files:
        st = p.stat()
        h.update(f"{p.name}:{st.st_size}".encode())
    return h.hexdigest()


def bronze_dir(source: str, dataset: str, day: str) -> Path:
    return layer("bronze", source, dataset, f"snapshot_date={day}")


class BronzeAgent(Agent):
    """Raw (jsonl.gz / csv) → parquet y nguyên, thêm cột lineage. Không đổi nội dung payload."""

    name = "bronze"

    def run(self, task: Task, ctx: Context) -> dict:
        source = task.params["source"]
        manifest_path = layer("bronze", source, "_manifest.json")
        manifest = read_json(manifest_path, {})
        stats = {"converted": 0, "unchanged": 0, "rows": 0}
        jobs = self.legacy_jobs(source) if source in LEGACY_SOURCES else self.crawl_jobs(source)
        for key, files, convert in jobs:
            sig = signature(files)
            if manifest.get(key, {}).get("sig") == sig:
                stats["unchanged"] += 1
                continue
            rows = convert()
            manifest[key] = {"sig": sig, "rows": rows, "built_at": now_iso()}
            write_json(manifest_path, manifest)
            stats["converted"] += 1
            stats["rows"] += rows
            self.say(ctx, task, f"{source}/{key}: {rows:,} dòng")
        return stats

    def crawl_jobs(self, source: str):
        for day in raw_days(source):
            for dataset, files in raw_datasets(source, day).items():
                yield f"{dataset}/{day}", files, lambda s=source, d=dataset, dy=day, f=files: self.write_jsonl(s, d, dy, f)

    def legacy_jobs(self, source: str):
        for name, dataset in LEGACY_TABLES[source]:
            f = RAW / source / name
            if f.exists():
                day = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).date().isoformat()
                yield f"{dataset}/{day}", [f], lambda s=source, d=dataset, dy=day, p=f: self.write_csv(s, d, dy, p)

    def write_jsonl(self, source: str, dataset: str, day: str, files: list[Path]) -> int:
        final = bronze_dir(source, dataset, day)
        tmp = final.with_name(final.name + ".tmp")
        tmp.mkdir(parents=True, exist_ok=True)
        base = RAW / source / day
        n = 0
        buf: list[dict] = []
        with pq.ParquetWriter(tmp / "part-0.parquet", BRONZE_SCHEMA, compression="zstd") as w:
            def flush():
                if buf:
                    w.write_table(pa.Table.from_pylist(buf, schema=BRONZE_SCHEMA))
                    buf.clear()

            for fp in files:
                rel = fp.relative_to(base).as_posix()
                for x in read_lines(fp):
                    buf.append({"_source": source, "_dataset": dataset, "_snapshot_date": date.fromisoformat(day),
                                "_crawl_ts": x.get("_crawl_ts"), "_file": rel,
                                "_req": json.dumps(x.get("_req"), ensure_ascii=False, sort_keys=True),
                                "payload": json.dumps(x.get("payload"), ensure_ascii=False)})
                    n += 1
                    if len(buf) >= BATCH:
                        flush()
            flush()
        swap_dir(tmp, final)
        return n

    def write_csv(self, source: str, dataset: str, day: str, csv: Path) -> int:
        final = bronze_dir(source, dataset, day)
        tmp = final.with_name(final.name + ".tmp")
        tmp.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect()
        q = csv.as_posix().replace("'", "''")
        con.execute(f"""
            COPY (SELECT *, '{source}' AS _source, '{dataset}' AS _dataset, DATE '{day}' AS _snapshot_date,
                         '{now_iso()}' AS _crawl_ts, '{csv.name}' AS _file
                  FROM read_csv('{q}', header=true, all_varchar=true)
            ) TO '{(tmp / "part-0.parquet").as_posix()}' (FORMAT parquet, COMPRESSION zstd)""")
        n = con.execute(f"SELECT count(*) FROM read_parquet('{(tmp / 'part-0.parquet').as_posix()}')").fetchone()[0]
        con.close()
        swap_dir(tmp, final)
        log.info("  bronze %s/%s: %s dòng", source, dataset, f"{n:,}")
        return n
