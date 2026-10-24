import json
import math
import mimetypes
import re
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

from lib.config import ROOT
from src.common.core import log

SAMPLE = ROOT / "data" / "sample"
STATIC = Path(__file__).parent / "static"
TABLES = ["dim_product", "dim_brand", "dim_category", "fact_media", "fact_price_snapshot"]
SORTS = {
    "name": "name", "price_asc": "price_vnd ASC NULLS LAST", "price_desc": "price_vnd DESC NULLS LAST",
    "images": "n_images DESC, name", "source": "source, category_l1, name",
}
LIST_COLS = ("product_key, source, name, brand_name, category_l1, category_l2, category_leaf, audience, "
             "price, original_price, price_currency, price_vnd, discount_pct, in_stock, img_local, image_url, n_images")
READ_ONLY = re.compile(r"^\s*(select|with|from|describe|show|explain|summarize)\b", re.I)
MAX_SQL_ROWS = 1000

CON: duckdb.DuckDBPyConnection


def load() -> duckdb.DuckDBPyConnection:
    if not (SAMPLE / "dim_product.parquet").is_file():
        raise SystemExit(f"Chưa có bộ mẫu ở {SAMPLE}. Chạy: python lake/scripts/make_sample.py")
    con = duckdb.connect()
    for t in TABLES:
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{(SAMPLE / (t + '.parquet')).as_posix()}')")
    # Một dòng mỗi sản phẩm: giá của snapshot mới nhất, ảnh đầu tiên và số ảnh, cho lưới và bộ lọc.
    con.execute("""CREATE TABLE pl AS
        SELECT p.product_key, p.source, p.name, p.audience_std AS audience, p.image_url, p.url, p.description,
               b.brand_name, p.category_l1, p.category_l2, c.category_leaf, c.category_path,
               f.price, f.original_price, f.currency AS price_currency, f.price_vnd, f.discount_pct, f.in_stock,
               m.img_local, coalesce(m.n_images, 0) AS n_images
        FROM dim_product p
        LEFT JOIN dim_brand b USING (brand_key) LEFT JOIN dim_category c USING (category_key)
        LEFT JOIN (SELECT * FROM fact_price_snapshot
                   QUALIFY row_number() OVER (PARTITION BY product_key ORDER BY date_key DESC) = 1) f USING (product_key)
        LEFT JOIN (SELECT product_key, arg_min(local_path, position) AS img_local, count(*) AS n_images
                   FROM fact_media WHERE media_type = 'image' GROUP BY product_key) m USING (product_key)""")
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    return con


def clean(v):
    if isinstance(v, float) and not math.isfinite(v):
        return None
    return v


def rows(cur, sql: str, args: list | None = None, limit: int | None = None) -> dict:
    cur.execute(sql, args or [])
    cols = [d[0] for d in cur.description]
    data = cur.fetchmany(limit) if limit else cur.fetchall()
    return {"columns": cols, "rows": [[clean(x) for x in r] for r in data]}


def records(cur, sql: str, args: list | None = None) -> list[dict]:
    r = rows(cur, sql, args)
    return [dict(zip(r["columns"], x)) for x in r["rows"]]


def facets(cur) -> dict:
    def col(c: str) -> list[dict]:
        return records(cur, f"SELECT {c} AS value, count(*) AS n FROM pl GROUP BY 1 ORDER BY n DESC, 1")
    return {"source": col("source"), "category_l1": col("category_l1"), "category_l2": col("category_l2"),
            "audience": col("audience")}


def overview(cur) -> dict:
    totals = records(cur, """SELECT (SELECT count(*) FROM pl) AS products,
        (SELECT count(*) FROM fact_media WHERE media_type = 'image') AS images,
        (SELECT count(DISTINCT source) FROM pl) AS sources,
        (SELECT count(DISTINCT category_l1) FROM pl) AS categories,
        (SELECT count(DISTINCT brand_name) FROM pl) AS brands""")[0]
    by_source = records(cur, """SELECT source, count(*) AS products, sum(n_images) AS images,
        round(avg(n_images), 1) AS avg_images, round(median(price_vnd)) AS median_price_vnd,
        round(avg(discount_pct), 1) AS avg_discount
        FROM pl GROUP BY source ORDER BY source""")
    matrix = records(cur, "SELECT source, category_l1, count(*) AS n FROM pl GROUP BY ALL ORDER BY source, category_l1")
    l2 = records(cur, "SELECT category_l1, category_l2, count(*) AS n FROM pl GROUP BY ALL ORDER BY category_l1, n DESC")
    return {"totals": totals, "by_source": by_source, "matrix": matrix, "l2": l2,
            "audience": records(cur, "SELECT audience AS value, count(*) AS n FROM pl GROUP BY 1 ORDER BY n DESC")}


def products(cur, q: dict) -> dict:
    g = lambda k: (q.get(k) or [""])[0].strip()
    where, args = ["TRUE"], []
    for col in ("source", "category_l1", "category_l2", "audience"):
        if g(col):
            where.append(f"{col} = ?")
            args.append(g(col))
    if g("q"):
        where.append("(name ILIKE ? OR brand_name ILIKE ? OR category_leaf ILIKE ?)")
        args += [f"%{g('q')}%"] * 3
    w = " AND ".join(where)
    size = min(int(g("size") or 48), 120)
    page = max(int(g("page") or 1), 1)
    total = cur.execute(f"SELECT count(*) FROM pl WHERE {w}", args).fetchone()[0]
    order = SORTS.get(g("sort"), SORTS["source"])
    items = records(cur, f"SELECT {LIST_COLS} FROM pl WHERE {w} ORDER BY {order}, product_key "
                         f"LIMIT {size} OFFSET {(page - 1) * size}", args)
    return {"total": total, "page": page, "size": size, "items": items}


def product(cur, key: str) -> dict:
    p = records(cur, """SELECT pl.*, d.attributes, d.currency, d.first_seen, d.last_seen, d.n_snapshots, d.audience_inferred
                        FROM pl JOIN dim_product d USING (product_key) WHERE product_key = ?""", [key])
    if not p:
        return {}
    return {
        "product": p[0],
        "media": records(cur, """SELECT media_type, role, position, color, url, local_path, width, height, bytes
                                 FROM fact_media WHERE product_key = ? ORDER BY position""", [key]),
        "history": records(cur, """SELECT date_key, price, original_price, price_vnd, discount_pct, in_stock
                                   FROM fact_price_snapshot WHERE product_key = ? ORDER BY date_key""", [key]),
    }


def run_sql(cur, sql: str) -> dict:
    sql = sql.strip().rstrip(";")
    if ";" in sql or not READ_ONLY.match(sql):
        return {"error": "chỉ cho phép một câu SELECT / WITH / DESCRIBE / SHOW"}
    t0 = time.monotonic()
    try:
        out = rows(cur, sql, limit=MAX_SQL_ROWS)
    except Exception as e:
        return {"error": str(e)}
    return {**out, "seconds": round(time.monotonic() - t0, 2), "capped": len(out["rows"]) == MAX_SQL_ROWS}


def json_safe(o):
    return str(o)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *a):
        pass

    def send(self, code: int, body: bytes, ctype: str, cache: str = "no-store") -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", cache)
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, obj, code: int = 200) -> None:
        self.send(code, json.dumps(obj, ensure_ascii=False, default=json_safe).encode("utf-8"),
                  "application/json; charset=utf-8")

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/":
                return self.send(200, (STATIC / "index.html").read_bytes(), "text/html; charset=utf-8")
            if u.path == "/favicon.ico":
                return self.send(204, b"", "image/x-icon")
            if u.path == "/img":
                return self.image((q.get("p") or [""])[0])
            cur = CON.cursor()
            if u.path == "/api/overview":
                return self.send_json(overview(cur))
            if u.path == "/api/facets":
                return self.send_json(facets(cur))
            if u.path == "/api/products":
                return self.send_json(products(cur, q))
            if u.path == "/api/product":
                return self.send_json(product(cur, (q.get("key") or [""])[0]))
            self.send(404, b"not found", "text/plain")
        except Exception as e:
            log.exception("viewer: lỗi %s", self.path)
            self.send_json({"error": str(e)}, 500)

    def do_POST(self):
        if urlparse(self.path).path != "/api/sql":
            return self.send(404, b"not found", "text/plain")
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            self.send_json(run_sql(CON.cursor(), body.get("sql", "")))
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def image(self, p: str) -> None:
        root = (SAMPLE / "images").resolve()
        path = (SAMPLE / p).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            return self.send(404, b"not found", "text/plain")
        self.send(200, path.read_bytes(), mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                  "public, max-age=86400")


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    global CON
    CON = load()
    srv = ThreadingHTTPServer((host, port), Handler)
    log.info("viewer: http://%s:%d (mẫu %s)", host, port, SAMPLE)
    srv.serve_forever()
