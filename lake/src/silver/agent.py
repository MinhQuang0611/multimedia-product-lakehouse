import csv
import shutil
from dataclasses import dataclass

import duckdb

from src.common.core import REPORTS, S, Agent, Context, Task, layer, now_iso, swap_dir
from src.common.schema import ENTITIES
from src.media.agent import manifest_path


@dataclass(frozen=True)
class Rule:
    code: str
    violated_if: str  # biểu thức SQL trên cột đã làm sạch


# Quy tắc cứng (vi phạm → quarantine) cho từng entity. Thiếu tên/giá/ảnh chỉ là cảnh báo mềm (dq_flags).
RULES = {
    "product": [
        Rule("PK_NULL", "product_key IS NULL OR source_product_id IS NULL"),
        Rule("PRICE_NEG", "price < 0 OR original_price < 0"),
        Rule("RATING_RANGE", "rating_avg < 0 OR rating_avg > 5"),
        Rule("DISCOUNT_RANGE", "discount_pct < 0 OR discount_pct > 100"),
    ],
    "variant": [Rule("PK_NULL", "variant_key IS NULL OR product_key IS NULL"), Rule("PRICE_NEG", "price < 0")],
    "media": [Rule("PK_NULL", "media_key IS NULL OR product_key IS NULL"),
              Rule("NO_LOCATION", "url IS NULL AND source_path IS NULL")],
    "review": [Rule("PK_NULL", "review_key IS NULL OR (product_key IS NULL AND order_key IS NULL)"),
               Rule("RATING_RANGE", "rating < 0 OR rating > 5"),
               Rule("EMPTY", "title IS NULL AND content IS NULL AND rating IS NULL")],
    "seller": [Rule("PK_NULL", "seller_key IS NULL")],
    "category": [Rule("PK_NULL", "category_key IS NULL")],
    "customer": [Rule("PK_NULL", "customer_key IS NULL"), Rule("AGE_RANGE", "age < 0 OR age > 120")],
    "order": [Rule("PK_NULL", "order_key IS NULL")],
    "order_item": [Rule("PK_NULL", "order_item_key IS NULL OR order_key IS NULL"), Rule("QTY_NONPOS", "quantity <= 0"),
                   Rule("PRICE_NEG", "unit_price < 0")],
    "payment": [Rule("PK_NULL", "payment_key IS NULL OR order_key IS NULL"), Rule("AMOUNT_NEG", "amount < 0")],
    "location": [Rule("PK_NULL", "location_key IS NULL")],
}
# Khoá + thứ tự chọn bản ghi mới nhất khi trùng.
DEDUP = {
    "product": ("product_key, snapshot_date", "crawl_ts DESC NULLS LAST, (price IS NOT NULL) DESC"),
    "variant": ("variant_key, snapshot_date", "price DESC NULLS LAST"),
    "media": ("media_key", "snapshot_date DESC"),
    "review": ("review_key", "snapshot_date DESC"),
    "seller": ("seller_key, snapshot_date", "name DESC NULLS LAST"),
    "category": ("category_key, snapshot_date", "name DESC NULLS LAST"),
    "customer": ("customer_key", "snapshot_date DESC"),
    "order": ("order_key", "snapshot_date DESC"),
    "order_item": ("order_item_key", "snapshot_date DESC"),
    "payment": ("payment_key", "snapshot_date DESC"),
    "location": ("location_key", "snapshot_date DESC"),
}
TEXT_COLS = {"product": ["name", "brand", "brand_std", "category_path", "category_leaf", "seller_name", "product_type"],
             "review": ["title", "content"], "variant": ["color", "size", "sku"]}


def parquet_glob(entity: str) -> str:
    return (layer("normalized", entity) / "*" / "*" / "*.parquet").as_posix()


def has_data(entity: str) -> bool:
    return any(layer("normalized", entity).glob("*/*/*.parquet"))


class SilverAgent(Agent):
    """Normalized → silver: làm sạch, khử trùng, kiểm tra chất lượng (lỗi vào quarantine), gắn đường dẫn media."""

    name = "silver"

    def run(self, task: Task, ctx: Context) -> dict:
        con = duckdb.connect()
        con.execute(f"SET temp_directory='{(S.agents_dir / 'duckdb_tmp').as_posix()}'")
        report = {}
        con.execute("SET preserve_insertion_order = false")
        for entity in ENTITIES:
            if not has_data(entity):
                continue
            report[entity] = self.build(con, entity)
            self.say(ctx, task, f"silver/{entity}: {report[entity]['valid']:,} hợp lệ, {report[entity]['quarantined']:,} quarantine")
        con.close()
        self.log_quality(report)
        return report

    def staged_sql(self, entity: str) -> str:
        src = f"read_parquet('{parquet_glob(entity)}', hive_partitioning=false, union_by_name=true)"
        repl = [f"nullif(trim({c}), '') AS {c}" for c in TEXT_COLS.get(entity, [])]
        if entity == "product":
            # Giá gốc chỉ có nghĩa khi cao hơn giá bán; % giảm tự tính nếu nguồn không cho.
            repl += ["CASE WHEN coalesce(rating_count, 0) = 0 AND coalesce(rating_avg, 0) = 0 THEN NULL ELSE rating_avg END AS rating_avg",
                     "CASE WHEN original_price > price THEN original_price END AS original_price",
                     "coalesce(discount_pct, CASE WHEN original_price > price AND price > 0 "
                     "THEN round(100.0 * (original_price - price) / original_price, 1) END) AS discount_pct"]
        # Tồn kho âm (hàng bán vượt kho trên Haravan) là dữ liệu thật: chặn về 0 và giữ cờ, không loại dòng.
        if entity in ("product", "variant"):
            repl.append("CASE WHEN stock_qty < 0 THEN 0 ELSE stock_qty END AS stock_qty")
        extra = ", (stock_qty < 0) AS stock_oversold" if entity == "product" else ""
        if entity == "product":
            # audience_std: nguồn không ghi đối tượng (phần lớn web hãng) thì lấy đối tượng chiếm ưu thế của cùng cửa hàng,
            # chỉ khi cửa hàng có ≥30 sản phẩm đã biết và ≥70% cùng một đối tượng; audience_inferred đánh dấu dòng được suy ra.
            return f"""WITH s AS (SELECT * REPLACE ({', '.join(repl)}){extra} FROM {src}),
                g AS (SELECT source, coalesce(seller_id, brand_std) AS grp, audience, count(*) AS n FROM s
                      WHERE audience <> 'unknown' AND coalesce(seller_id, brand_std) IS NOT NULL GROUP BY ALL),
                t AS (SELECT source, grp, sum(n) AS n_known, max_by(audience, n) AS top, max(n)::DOUBLE / sum(n) AS purity
                      FROM g GROUP BY source, grp)
                SELECT s.*, CASE WHEN s.audience <> 'unknown' THEN s.audience
                                 WHEN t.n_known >= 30 AND t.purity >= 0.7 THEN t.top ELSE 'unknown' END AS audience_std,
                       (s.audience = 'unknown' AND t.n_known >= 30 AND t.purity >= 0.7) AS audience_inferred
                FROM s LEFT JOIN t ON s.source = t.source AND coalesce(s.seller_id, s.brand_std) = t.grp"""
        if entity != "media":
            return f"SELECT * REPLACE ({', '.join(repl)}) FROM {src}" if repl else f"SELECT * FROM {src}"
        files = sorted(manifest_path("x").parent.glob("*.jsonl")) if manifest_path("x").parent.exists() else []
        cols = ("media_key VARCHAR, status VARCHAR, local_path VARCHAR, external BOOLEAN, sha256 VARCHAR, "
                "bytes BIGINT, width INTEGER, height INTEGER, content_type VARCHAR, fetched_at VARCHAR")
        if files:
            lst = ", ".join(f"'{f.as_posix()}'" for f in files)
            spec = ", ".join("'%s': '%s'" % tuple(c.split()) for c in cols.split(", "))
            mf = (f"(SELECT * FROM read_json([{lst}], format='newline_delimited', columns={{{spec}}}) "
                  f"QUALIFY row_number() OVER (PARTITION BY media_key ORDER BY fetched_at DESC) = 1)")
        else:
            mf = "(SELECT " + ", ".join(f"NULL::{t} AS {n}" for n, t in (c.split() for c in cols.split(", "))) + " WHERE false)"
        return (f"SELECT m.* EXCLUDE (width, height), coalesce(f.width, m.width) AS width, coalesce(f.height, m.height) AS height, "
                f"f.status AS download_status, f.local_path, f.external, f.sha256, f.bytes, f.content_type "
                f"FROM {src} m LEFT JOIN {mf} f USING (media_key)")

    def build(self, con, entity: str) -> dict:
        key, order = DEDUP[entity]
        rules = RULES[entity]
        flags = ", ".join(f"CASE WHEN coalesce(({r.violated_if}), false) THEN '{r.code}' END" for r in rules)
        extra = ""
        if entity == "product":
            extra = (", list_filter([CASE WHEN name IS NULL THEN 'NO_NAME' END, CASE WHEN price IS NULL THEN 'NO_PRICE' END,"
                     " CASE WHEN image_count IS NULL OR image_count = 0 THEN 'NO_IMAGE' END,"
                     " CASE WHEN category_leaf IS NULL THEN 'NO_CATEGORY' END], x -> x IS NOT NULL) AS dq_flags")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE _chk AS
            SELECT *, list_filter([{flags}], x -> x IS NOT NULL) AS _errors{extra}
            FROM ({self.staged_sql(entity)})
            QUALIFY row_number() OVER (PARTITION BY {key} ORDER BY {order}) = 1""")
        tmp, final = layer(f"_tmp_silver_{entity}"), layer("silver", entity)
        qtmp, qfinal = layer(f"_tmp_quarantine_{entity}"), layer("silver", "_quarantine", entity)
        for d in (tmp, qtmp):
            shutil.rmtree(d, ignore_errors=True)
        copy = "(FORMAT parquet, COMPRESSION zstd, PARTITION_BY (source), OVERWRITE_OR_IGNORE true)"
        con.execute(f"COPY (SELECT * EXCLUDE (_errors) FROM _chk WHERE len(_errors) = 0) TO '{tmp.as_posix()}' {copy}")
        con.execute(f"COPY (SELECT * FROM _chk WHERE len(_errors) > 0) TO '{qtmp.as_posix()}' {copy}")
        ok = con.execute("SELECT count(*) FROM _chk WHERE len(_errors) = 0").fetchone()[0]
        bad = con.execute("SELECT count(*) FROM _chk WHERE len(_errors) > 0").fetchone()[0]
        by_code = dict(con.execute("SELECT e, count(*) FROM (SELECT unnest(_errors) e FROM _chk) GROUP BY e ORDER BY e").fetchall())
        for t, f in ((tmp, final), (qtmp, qfinal)):
            if t.exists():
                swap_dir(t, f)
            else:
                shutil.rmtree(f, ignore_errors=True)
        con.execute("DROP TABLE _chk")
        return {"valid": ok, "quarantined": bad, "errors": by_code}

    @staticmethod
    def log_quality(report: dict) -> None:
        out = REPORTS / "quality_log.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        new = not out.exists()
        with open(out, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["run_ts", "table", "valid", "quarantined", "errors"])
            for e, r in report.items():
                w.writerow([now_iso(), f"silver/{e}", r["valid"], r["quarantined"],
                            ";".join(f"{k}={v}" for k, v in r["errors"].items())])
