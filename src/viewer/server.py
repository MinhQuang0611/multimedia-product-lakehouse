import json
import mimetypes
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb

from src.common.core import LAKE, RAW, S, log
from src.common.schema import ENTITIES

GOLD = LAKE / "gold"
BIG_ROWS = 5_000_000  # bảng lớn hơn thế (giao dịch H&M) để dạng view trên parquet, không nạp vào RAM
STATIC = Path(__file__).parent / "static"
IMG_ROOTS = [(LAKE / "media").resolve(), (RAW / "hm").resolve(), (RAW / "shopee").resolve()]
SORTS = {
    "name": "name", "price_asc": "price_vnd ASC NULLS LAST", "price_desc": "price_vnd DESC NULLS LAST",
    "discount": "discount_pct DESC NULLS LAST", "rating": "rating_avg DESC NULLS LAST, rating_count DESC NULLS LAST",
    "sold": "sold_count DESC NULLS LAST", "newest": "last_seen DESC, product_key",
}
LIST_COLS = ("product_key, source, name, brand_name, category_l1, category_l2, category_leaf, audience, price, original_price, price_currency, "
             "price_vnd, discount_pct, in_stock, rating_avg, rating_count, sold_count, image_url, img_local")
READ_ONLY = re.compile(r"^\s*(select|with|from|describe|show|explain|summarize)\b", re.I)


def gold_version() -> float:
    files = list(GOLD.glob("*/data.parquet"))
    return max((f.stat().st_mtime for f in files), default=0.0)


class Store:
    """Nạp toàn bộ gold vào DuckDB trong RAM, tự nạp lại khi pipeline ghi bản gold mới."""

    def __init__(self):
        self.lock = threading.Lock()
        self.version = 0.0
        self.con = None
        self.tables: list[str] = []
        self.reload()

    def reload(self) -> None:
        t0 = time.monotonic()
        con = duckdb.connect()
        names = sorted(p.parent.name for p in GOLD.glob("*/data.parquet"))
        for n in names:
            f = (GOLD / n / "data.parquet").as_posix()
            big = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0] > BIG_ROWS
            con.execute(f"CREATE {'VIEW' if big else 'TABLE'} {n} AS SELECT * FROM read_parquet('{f}')")
        if "dim_product" in names:
            # Bảng phẳng một dòng mỗi sản phẩm (giá của snapshot mới nhất + ảnh chính) cho trang danh sách.
            con.execute("""CREATE TABLE pl AS
                SELECT p.product_key, p.source, p.name, p.audience_std AS audience, p.image_url, p.last_seen, p.first_seen,
                       b.brand_name, p.category_l1, p.category_l2, c.category_leaf, c.category_path, f.price, f.original_price, f.currency AS price_currency,
                       f.price_vnd, f.discount_pct, f.in_stock, f.stock_qty, f.rating_avg, f.rating_count, f.sold_count,
                       m.local_path AS img_local
                FROM dim_product p
                LEFT JOIN dim_brand b USING (brand_key) LEFT JOIN dim_category c USING (category_key)
                LEFT JOIN (SELECT * FROM fact_price_snapshot
                           QUALIFY row_number() OVER (PARTITION BY product_key ORDER BY date_key DESC) = 1) f USING (product_key)
                LEFT JOIN (SELECT product_key, arg_min(local_path, position) AS local_path FROM fact_media
                           WHERE download_status = 'ok' AND media_type = 'image' GROUP BY product_key) m USING (product_key)""")
            names.append("pl")
        # Ô SQL chỉ được đọc file trong thư mục gold (cần cho view của bảng lớn), không đọc/ghi chỗ khác.
        con.execute(f"SET allowed_directories = ['{GOLD.as_posix()}']")
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
        with self.lock:
            self.con, self.tables, self.version = con, names, gold_version()
        log.info("viewer: nạp %d bảng gold trong %.1fs", len(names), time.monotonic() - t0)

    def cursor(self):
        if gold_version() > self.version:
            try:
                self.reload()
            except Exception as e:
                log.warning("viewer: nạp lại lỗi (%s), giữ bản cũ", e)
        with self.lock:
            return self.con.cursor()


STORE: Store


def rows(cur, sql: str, args: list | None = None, limit: int | None = None) -> dict:
    cur.execute(sql, args or [])
    cols = [d[0] for d in cur.description]
    data = cur.fetchmany(limit) if limit else cur.fetchall()
    return {"columns": cols, "rows": [list(r) for r in data]}


def records(cur, sql: str, args: list | None = None) -> list[dict]:
    r = rows(cur, sql, args)
    return [dict(zip(r["columns"], x)) for x in r["rows"]]


def overview(cur) -> dict:
    src = records(cur, """
        SELECT s.source, s.source_name, s.source_type, s.country, s.data_types,
               count(f.product_key) AS products, round(median(f.price_vnd)) AS median_price_vnd,
               round(avg(f.discount_pct), 1) AS avg_discount, round(100 * avg(f.in_stock::INT)) AS in_stock_pct,
               min(f.date_key) AS first_date, max(f.date_key) AS last_date
        FROM dim_source s LEFT JOIN (SELECT * FROM fact_price_snapshot QUALIFY row_number() OVER (
             PARTITION BY product_key ORDER BY date_key DESC) = 1) f USING (source)
        GROUP BY ALL ORDER BY products DESC""")
    media = {r["source"]: r for r in records(cur, f"""
        SELECT source, count(*) FILTER (WHERE \"position\" < {S.media_max_per_product}) AS media_total,
               count(*) FILTER (WHERE download_status = 'ok' AND \"position\" < {S.media_max_per_product}) AS media_ok, count(*) AS media_all
        FROM fact_media GROUP BY source""")}
    reviews = {r["source"]: r["n"] for r in records(cur, "SELECT source, count(*) AS n FROM fact_review GROUP BY source")}
    def counts(table: str) -> dict:
        return {r["source"]: r["n"] for r in records(cur, f"SELECT source, count(*) AS n FROM {table} GROUP BY source")}

    orders, customers = counts("fact_order"), counts("dim_customer")
    for s in src:
        s.update(media.get(s["source"], {"media_total": 0, "media_ok": 0}))
        s["reviews"] = reviews.get(s["source"], 0)
        s["orders"], s["customers"] = orders.get(s["source"], 0), customers.get(s["source"], 0)
    tables = [{"name": n, "rows": cur.execute(f"SELECT count(*) FROM {n}").fetchone()[0]} for n in STORE.tables if n != "pl"]
    return {"sources": src, "tables": tables, "version": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(STORE.version))}


def products(cur, q: dict) -> dict:
    g = lambda k: (q.get(k) or [""])[0].strip()
    where, args = ["TRUE"], []
    for col in ("source", "audience"):
        if g(col):
            where.append(f"{col} = ?")
            args.append(g(col))
    if g("q"):
        where.append("(name ILIKE ? OR brand_name ILIKE ?)")
        args += [f"%{g('q')}%"] * 2
    if g("category"):
        where.append("(category_leaf ILIKE ? OR category_path ILIKE ? OR category_l1 = ? OR category_l2 = ?)")
        args += [f"%{g('category')}%"] * 2 + [g("category")] * 2
    for key, op in (("min", ">="), ("max", "<=")):
        if g(key):
            where.append(f"price_vnd {op} ?")
            args.append(float(g(key)))
    if g("instock") == "1":
        where.append("in_stock")
    if g("hasimg") == "1":
        where.append("(img_local IS NOT NULL OR image_url IS NOT NULL)")
    w = " AND ".join(where)
    size = min(int(g("size") or 40), 100)
    page = max(int(g("page") or 1), 1)
    total = cur.execute(f"SELECT count(*) FROM pl WHERE {w}", args).fetchone()[0]
    order = SORTS.get(g("sort"), SORTS["newest"])
    items = records(cur, f"SELECT {LIST_COLS} FROM pl WHERE {w} ORDER BY {order} LIMIT {size} OFFSET {(page - 1) * size}", args)
    return {"total": total, "page": page, "size": size, "items": items}


def product(cur, key: str) -> dict:
    p = records(cur, """SELECT pl.*, d.description, d.url, d.attributes, d.seller_id, d.currency AS currency, d.n_snapshots
                        FROM pl JOIN dim_product d USING (product_key) WHERE product_key = ?""", [key])
    if not p:
        return {}
    p = p[0]
    return {
        "product": p,
        "variants": records(cur, """SELECT color, size, sku, price, currency, in_stock, stock_qty FROM fact_variant_stock
            WHERE product_key = ? AND date_key = (SELECT max(date_key) FROM fact_variant_stock WHERE product_key = ?)
            ORDER BY color, size LIMIT 200""", [key, key]),
        "media": records(cur, """SELECT media_type, role, position, color, url, local_path, download_status, width, height
            FROM fact_media WHERE product_key = ? ORDER BY position LIMIT 60""", [key]),
        "history": records(cur, """SELECT date_key, price, original_price, price_vnd, discount_pct, in_stock, stock_qty, sold_count
            FROM fact_price_snapshot WHERE product_key = ? ORDER BY date_key""", [key]),
        "reviews": records(cur, """SELECT date_key, rating, title, content, verified_purchase, has_photo, size_purchased, color_purchased
            FROM fact_review WHERE product_key = ? ORDER BY date_key DESC NULLS LAST LIMIT 20""", [key]),
        "seller": (records(cur, "SELECT * FROM dim_seller WHERE seller_id = ? AND source = ?", [p["seller_id"], p["source"]])
                   if p.get("seller_id") else []),
    }


def table(cur, name: str, page: int) -> dict:
    if name not in STORE.tables:
        return {"error": "không có bảng này"}
    total = cur.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
    out = rows(cur, f"SELECT * FROM {name} LIMIT 100 OFFSET {(page - 1) * 100}")
    return {**out, "total": total, "page": page}


def run_sql(cur, sql: str) -> dict:
    sql = sql.strip().rstrip(";")
    if ";" in sql or not READ_ONLY.match(sql):
        return {"error": "chỉ cho phép một câu SELECT / WITH / DESCRIBE / SHOW"}
    t0 = time.monotonic()
    try:
        out = rows(cur, sql, limit=1000)
    except Exception as e:
        return {"error": str(e)}
    return {**out, "seconds": round(time.monotonic() - t0, 2), "capped": len(out["rows"]) == 1000}


SCHEMA_CACHE: dict = {}


def silver_src(entity: str) -> str | None:
    d = LAKE / "silver" / entity
    if not any(d.glob("*/*.parquet")):
        return None
    return f"read_parquet('{(d / '*' / '*.parquet').as_posix()}', hive_partitioning=true, union_by_name=true)"


def schema(entity: str) -> dict:
    """Danh sách entity chuẩn, hoặc chi tiết một entity: cột, kiểu, ví dụ và % có dữ liệu theo nguồn (đọc từ silver)."""
    if not entity:
        out = []
        for name, cols in ENTITIES.items():
            src = silver_src(name)
            if not src:
                out.append({"name": name, "columns": len(cols), "rows": 0, "sources": []})
                continue
            con = duckdb.connect()
            by = con.execute(f"SELECT source, count(*) FROM {src} GROUP BY 1 ORDER BY 2 DESC").fetchall()
            out.append({"name": name, "columns": len(cols), "rows": sum(n for _, n in by), "sources": [s for s, _ in by]})
            con.close()
        return {"entities": out}
    if entity not in ENTITIES:
        return {"error": "không có entity này"}
    key = (entity, STORE.version)
    if key not in SCHEMA_CACHE:
        src = silver_src(entity)
        if not src:
            return {"error": "chưa có dữ liệu silver"}
        con = duckdb.connect()
        desc = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
        names = [d[0] for d in desc if d[0] != "source"]
        agg = ", ".join(f'count("{c}") AS "n_{i}"' for i, c in enumerate(names))
        ex = ", ".join(f'any_value("{c}") FILTER (WHERE "{c}" IS NOT NULL) AS "e_{i}"' for i, c in enumerate(names))
        res = con.execute(f"SELECT source, count(*) AS n, {agg} FROM {src} GROUP BY source ORDER BY n DESC").fetchall()
        example = con.execute(f"SELECT {ex} FROM {src}").fetchone()
        base = {c for c, _ in ENTITIES[entity]}
        con.close()
        sources = [{"source": r[0], "rows": r[1]} for r in res]
        cols = []
        for i, c in enumerate(names):
            typ = next(d[1] for d in desc if d[0] == c)
            cols.append({"name": c, "type": typ, "core": c in base, "example": example[i],
                         "fill": {r[0]: round(100 * r[2 + i] / r[1], 1) for r in res}})
        SCHEMA_CACHE[key] = {"entity": entity, "sources": sources, "columns": cols}
    return SCHEMA_CACHE[key]


def sample(entity: str, source: str, n: int) -> dict:
    src = silver_src(entity) if entity in ENTITIES else None
    if not src:
        return {"error": "chưa có dữ liệu silver"}
    con = duckdb.connect()
    try:
        return rows(con, f"SELECT * FROM {src} WHERE source = ? LIMIT {min(max(n, 1), 200)}", [source])
    finally:
        con.close()


def json_safe(o):
    if isinstance(o, float) and o != o:
        return None
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
        self.send(code, json.dumps(obj, ensure_ascii=False, default=json_safe, allow_nan=False).encode("utf-8"),
                  "application/json; charset=utf-8")

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path == "/":
                return self.send(200, (STATIC / "index.html").read_bytes(), "text/html; charset=utf-8")
            if u.path == "/img":
                return self.image((q.get("p") or [""])[0])
            cur = STORE.cursor()
            if u.path == "/api/overview":
                return self.send_json(overview(cur))
            if u.path == "/api/products":
                return self.send_json(products(cur, q))
            if u.path == "/api/product":
                return self.send_json(product(cur, (q.get("key") or [""])[0]))
            if u.path == "/api/schema":
                return self.send_json(schema((q.get("entity") or [""])[0]))
            if u.path == "/api/sample":
                return self.send_json(sample((q.get("entity") or [""])[0], (q.get("source") or [""])[0], int((q.get("n") or ["20"])[0])))
            if u.path == "/api/table":
                return self.send_json(table(cur, (q.get("name") or [""])[0], int((q.get("page") or ["1"])[0])))
            self.send(404, b"not found", "text/plain")
        except Exception as e:
            log.exception("viewer: lỗi %s", self.path)
            self.send_json({"error": str(e)}, 500)

    def do_POST(self):
        if urlparse(self.path).path != "/api/sql":
            return self.send(404, b"not found", "text/plain")
        try:
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            self.send_json(run_sql(STORE.cursor(), body.get("sql", "")))
        except Exception as e:
            self.send_json({"error": str(e)}, 500)

    def image(self, p: str) -> None:
        path = (Path(p) if p.startswith("/") else LAKE / p).resolve()
        if not any(path.is_relative_to(r) for r in IMG_ROOTS) or not path.is_file():
            return self.send(404, b"not found", "text/plain")
        self.send(200, path.read_bytes(), mimetypes.guess_type(path.name)[0] or "application/octet-stream",
                  "public, max-age=86400")


def serve(host: str = "127.0.0.1", port: int = 8765) -> None:
    global STORE
    STORE = Store()
    srv = ThreadingHTTPServer((host, port), Handler)
    log.info("viewer: http://%s:%d", host, port)
    srv.serve_forever()
