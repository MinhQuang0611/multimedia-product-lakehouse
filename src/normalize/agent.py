import json
import shutil

import duckdb

import pyarrow.parquet as pq

from src.normalize.adapters import brands, legacy, tiki, uniqlo, zara
from src.normalize.standardize import clean_brand, std_category
from src.normalize.adapters.common import to_date
from src.bronze.agent import bronze_dir
from src.common.core import LEGACY_SOURCES, Agent, Context, Task, layer, now_iso, read_json, swap_dir, write_json
from src.common.schema import ENTITIES, SCHEMA_VERSION, arrow_schema, to_table

FLUSH = 50_000
ADAPTERS = {"tiki": tiki.normalize, "brands": brands.normalize, "zara": zara.normalize, "uniqlo": uniqlo.normalize}


class Bronze:
    """Đọc bronze của một (nguồn, ngày); payload của nguồn crawl được giải JSON lại."""

    def __init__(self, source: str, day: str):
        self.source, self.day = source, day

    def datasets(self) -> list[str]:
        root = layer("bronze", self.source)
        return sorted(p.name for p in root.iterdir() if (p / f"snapshot_date={self.day}").is_dir()) if root.exists() else []

    def rows(self, dataset: str):
        d = bronze_dir(self.source, dataset, self.day)
        if not d.exists():
            return
        legacy_src = self.source in LEGACY_SOURCES
        for f in sorted(d.glob("*.parquet")):
            for batch in pq.ParquetFile(f).iter_batches(batch_size=2000):
                for row in batch.to_pylist():
                    if legacy_src:
                        yield row
                    else:
                        yield {"req": json.loads(row["_req"]), "payload": json.loads(row["payload"]),
                               "crawl_ts": row["_crawl_ts"], "file": row["_file"]}


class Sink:
    """Gom dòng theo entity và ghi parquet từng đợt, không giữ cả nguồn trong RAM."""

    def __init__(self, source: str, day: str, tmp_root):
        self.source, self.day, self.snap = source, day, to_date(day)
        self.root = tmp_root
        self.buf: dict[str, list[dict]] = {e: [] for e in ENTITIES}
        self.writers: dict[str, pq.ParquetWriter] = {}
        self.counts = {e: 0 for e in ENTITIES}

    def emit(self, entity: str, row: dict) -> None:
        row["source"] = self.source
        row["snapshot_date"] = self.snap
        if entity == "product":
            # Làm sạch brand và gán danh mục chuẩn ở một chỗ để mọi nguồn dùng chung một bộ luật.
            row["brand_std"] = clean_brand(row.get("brand"))
            row["category_l1"], row["category_l2"] = std_category(
                self.source, row.get("category_path"), row.get("product_type"), row.get("name"))
        buf = self.buf[entity]
        buf.append(row)
        if len(buf) >= FLUSH:
            self.flush(entity)

    def flush(self, entity: str) -> None:
        rows = self.buf[entity]
        if not rows:
            return
        if entity not in self.writers:
            out = self.root / entity
            out.mkdir(parents=True, exist_ok=True)
            self.writers[entity] = pq.ParquetWriter(out / "part-0.parquet", arrow_schema(entity), compression="zstd")
        self.writers[entity].write_table(to_table(entity, rows))
        self.counts[entity] += len(rows)
        rows.clear()

    def close(self) -> dict[str, int]:
        for e in ENTITIES:
            self.flush(e)
        for w in self.writers.values():
            w.close()
        return self.counts


class NormalizeAgent(Agent):
    """Bronze → schema chuẩn chung (product/variant/media/review/seller), mỗi nguồn một adapter."""

    name = "normalize"

    def run(self, task: Task, ctx: Context) -> dict:
        source = task.params["source"]
        bronze_manifest = read_json(layer("bronze", source, "_manifest.json"), {})
        manifest_path = layer("normalized", f"_manifest_{source}.json")  # một file mỗi nguồn: các nguồn chạy song song
        manifest = read_json(manifest_path, {})
        days: dict[str, dict[str, str]] = {}
        for key, m in bronze_manifest.items():
            ds, day = key.split("/")
            days.setdefault(day, {})[ds] = m["sig"]
        out = {"days": 0, "unchanged": 0, "rows": {e: 0 for e in ENTITIES}}
        fn = legacy.normalize(source) if source in LEGACY_SOURCES else ADAPTERS[source]
        for day in sorted(days):
            sig = json.dumps({"schema": SCHEMA_VERSION, **days[day]}, sort_keys=True)
            mk = f"{source}/{day}"
            if manifest.get(mk, {}).get("sig") == sig:
                out["unchanged"] += 1
                continue
            tmp = layer("normalized", f"_tmp_{source}_{day}")
            sink = Sink(source, day, tmp)
            bronze = Bronze(source, day)
            fn(bronze, day, sink.emit)
            counts = sink.close()
            counts.update(self.run_sql(source, day, bronze, tmp))
            for e in ENTITIES:
                final = layer("normalized", e, f"source={source}", f"snapshot_date={day}")
                if (tmp / e).exists():
                    swap_dir(tmp / e, final)
                else:
                    shutil.rmtree(final, ignore_errors=True)  # ngày được chuẩn hoá lại mà entity hết dòng
            shutil.rmtree(tmp, ignore_errors=True)
            manifest[mk] = {"sig": sig, "counts": counts, "built_at": now_iso()}
            write_json(manifest_path, manifest)
            out["days"] += 1
            for e, n in counts.items():
                out["rows"][e] += n
            self.say(ctx, task, f"{mk}: " + ", ".join(f"{e}={n:,}" for e, n in counts.items() if n))
        return out

    @staticmethod
    def run_sql(source: str, day: str, bronze: Bronze, tmp) -> dict[str, int]:
        """Bảng lớn (giao dịch H&M, địa điểm Olist) chuẩn hoá bằng SQL thẳng từ bronze sang parquet."""
        build = legacy.SQL.get(source)
        counts = {}
        if not build:
            return counts
        con = duckdb.connect()
        con.execute("SET preserve_insertion_order = false")
        for entity, sql in build(bronze, day).items():
            out = tmp / entity
            out.mkdir(parents=True, exist_ok=True)
            con.execute(f"COPY ({sql}) TO '{(out / 'part-0.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
            counts[entity] = con.execute(f"SELECT count(*) FROM read_parquet('{(out / 'part-0.parquet').as_posix()}')").fetchone()[0]
        con.close()
        return counts
