import shutil

import duckdb

from src.common.core import S, Agent, Context, Task, layer, swap_dir
from src.search.catalog import feature_views

SOURCE_INFO = [  # source, tên, loại, quốc gia, loại dữ liệu
    ("tiki", "Tiki", "marketplace", "VN", "product,variant,image,review,seller,category"),
    ("brands", "Web hãng thời trang", "brand-web", "VN/US", "product,variant,image,seller,category"),
    ("zara", "Zara", "retailer", "VN", "product,variant,image,video,seller,category"),
    ("uniqlo", "Uniqlo", "retailer", "US", "product,variant,image,review,seller,category"),
    ("hm", "H&M (Kaggle)", "dataset", "SE", "product,variant,image,customer,order,order_item,category"),
    ("shopee", "Shopee (Kaggle)", "dataset", "ID", "product,image"),
    ("olist", "Olist (Kaggle)", "dataset", "BR", "product,customer,order,order_item,payment,review,seller,location,category"),
]


def silver(entity: str) -> str:
    return f"read_parquet('{(layer('silver', entity) / '*' / '*.parquet').as_posix()}', hive_partitioning=true, union_by_name=true)"


def exists(entity: str) -> bool:
    return any(layer("silver", entity).glob("*/*.parquet"))


class GoldAgent(Agent):
    """Silver → gold: lược đồ sao (dim/fact) có giá quy đổi VND, cộng vài bảng tổng hợp để phân tích."""

    name = "gold"

    def run(self, task: Task, ctx: Context) -> dict:
        if not exists("product"):
            return {"tables": 0, "note": "chưa có silver/product"}
        con = duckdb.connect()
        con.execute(f"SET temp_directory='{(S.agents_dir / 'duckdb_tmp').as_posix()}'")
        con.execute("SET preserve_insertion_order = false")
        fx = ", ".join(f"('{k}', {v})" for k, v in S.fx_to_vnd.items())
        src = ", ".join(f"('{a}','{b}','{c}','{d}','{e}')" for a, b, c, d, e in SOURCE_INFO)
        con.execute(f"CREATE TEMP TABLE fx(currency, to_vnd) AS VALUES {fx}")
        con.execute(f"CREATE TEMP TABLE p AS SELECT * FROM {silver('product')}")
        con.execute("""CREATE TEMP TABLE pv AS
            SELECT p.*, p.price * fx.to_vnd AS price_vnd, p.original_price * fx.to_vnd AS original_price_vnd,
                   md5(lower(strip_accents(trim(p.brand_std)))) AS brand_key,
                   md5(p.source || '|' || coalesce(p.category_path, '')) AS category_key
            FROM p LEFT JOIN fx ON p.currency = fx.currency""")
        has = {e: exists(e) for e in ("media", "variant", "review", "seller", "category", "customer", "order",
                                      "order_item", "payment", "location")}
        if has["media"]:
            con.execute(f"CREATE TEMP TABLE m AS SELECT * FROM {silver('media')}")
        if has["variant"]:
            con.execute(f"CREATE TEMP TABLE v AS SELECT * FROM {silver('variant')}")
        lo, hi = con.execute("SELECT min(snapshot_date), max(snapshot_date) FROM p").fetchone()
        for sql in (f"SELECT min(created_at)::DATE, max(created_at)::DATE FROM {silver('review')}" if has["review"] else None,
                    f"SELECT min(order_ts)::DATE, max(order_ts)::DATE FROM {silver('order')}" if has["order"] else None):
            if sql:
                a, b = con.execute(sql).fetchone()
                lo, hi = min(lo, a or lo), max(hi, b or hi)

        t: dict[str, str] = {
            "dim_source": f"SELECT * FROM (VALUES {src}) t(source, source_name, source_type, country, data_types)",
            "dim_date": f"""SELECT d::DATE AS date_key, year(d) AS year, quarter(d) AS quarter, month(d) AS month, day(d) AS day,
                                   dayofweek(d) AS weekday FROM generate_series(DATE '{lo}', DATE '{hi}', INTERVAL 1 DAY) t(d)""",
            "dim_brand": """SELECT brand_key, mode(trim(brand_std)) AS brand_name, count(DISTINCT product_key) AS n_products,
                                   list(DISTINCT source) AS sources
                            FROM pv WHERE brand_std IS NOT NULL GROUP BY brand_key""",
            "dim_category": """SELECT category_key, source, category_path,
                                      string_split(category_path, ' > ')[1] AS level1, string_split(category_path, ' > ')[2] AS level2,
                                      string_split(category_path, ' > ')[3] AS level3, any_value(category_leaf) AS category_leaf,
                                      mode(category_l1) AS category_l1, mode(category_l2) AS category_l2,
                                      mode(audience_std) AS audience, count(DISTINCT product_key) AS n_products
                               FROM pv GROUP BY category_key, source, category_path""",
            "dim_category_std": """SELECT category_l1, category_l2, count(DISTINCT product_key) AS n_products,
                                          list(DISTINCT source) AS sources FROM pv GROUP BY category_l1, category_l2""",
            # Một dòng mỗi sản phẩm: thuộc tính mô tả lấy giá trị không-null mới nhất qua các snapshot.
            "dim_product": """SELECT product_key, any_value(source) AS source, any_value(source_product_id) AS source_product_id,
                    arg_max(name, snapshot_date) FILTER (WHERE name IS NOT NULL) AS name,
                    arg_max(brand_key, snapshot_date) FILTER (WHERE brand_key IS NOT NULL) AS brand_key,
                    arg_max(category_key, snapshot_date) AS category_key,
                    arg_max(category_l1, snapshot_date) AS category_l1, arg_max(category_l2, snapshot_date) AS category_l2,
                    arg_max(description, snapshot_date) FILTER (WHERE description IS NOT NULL) AS description,
                    arg_max(audience, snapshot_date) AS audience, arg_max(audience_std, snapshot_date) AS audience_std,
                    arg_max(audience_inferred, snapshot_date) AS audience_inferred, arg_max(currency, snapshot_date) AS currency,
                    arg_max(url, snapshot_date) FILTER (WHERE url IS NOT NULL) AS url,
                    arg_max(seller_id, snapshot_date) FILTER (WHERE seller_id IS NOT NULL) AS seller_id,
                    arg_max(attributes, snapshot_date) FILTER (WHERE attributes IS NOT NULL) AS attributes,
                    arg_max(image_url, snapshot_date) FILTER (WHERE image_url IS NOT NULL) AS image_url,
                    min(snapshot_date) AS first_seen, max(snapshot_date) AS last_seen, count(DISTINCT snapshot_date) AS n_snapshots
                FROM pv GROUP BY product_key""",
            "fact_price_snapshot": """SELECT product_key, snapshot_date AS date_key, source, currency, price, original_price,
                    price_vnd, original_price_vnd, discount_pct, in_stock, stock_qty, sold_count, rating_avg, rating_count,
                    variant_count, image_count,
                    price_vnd - lag(price_vnd) OVER (PARTITION BY product_key ORDER BY snapshot_date) AS price_vnd_change
                FROM pv""",
        }
        if has["seller"]:
            t["dim_seller"] = f"""SELECT seller_key, source, seller_id, arg_max(name, snapshot_date) AS name,
                    arg_max(rating, snapshot_date) AS rating, arg_max(review_count, snapshot_date) AS review_count,
                    arg_max(follower_count, snapshot_date) AS follower_count, arg_max(is_official, snapshot_date) AS is_official,
                    arg_max(url, snapshot_date) AS url, arg_max(country, snapshot_date) AS country,
                    arg_max(city, snapshot_date) AS city, arg_max(region, snapshot_date) AS region,
                    arg_max(postal_prefix, snapshot_date) AS postal_prefix
                FROM {silver('seller')} GROUP BY seller_key, source, seller_id"""
        if has["category"]:
            t["dim_category_tree"] = f"""SELECT category_key, source, source_category_id, arg_max(parent_key, snapshot_date) AS parent_key,
                    arg_max(name, snapshot_date) AS name, arg_max(level, snapshot_date) AS level,
                    arg_max(path, snapshot_date) AS path, arg_max(url, snapshot_date) AS url
                FROM {silver('category')} GROUP BY category_key, source, source_category_id"""
        if has["variant"]:
            t["fact_variant_stock"] = """SELECT v.variant_key, v.product_key, v.source, v.snapshot_date AS date_key, v.sku, v.color, v.size,
                    v.price, v.price * fx.to_vnd AS price_vnd, v.currency, v.in_stock, v.stock_qty
                FROM v LEFT JOIN fx ON v.currency = fx.currency"""
        if has["media"]:
            t["fact_media"] = """SELECT media_key, product_key, source, media_type, role, position, url, color, width, height,
                    download_status, local_path, external, sha256, bytes, content_type FROM m"""
        if has["review"]:
            t["fact_review"] = f"""SELECT review_key, product_key, order_key, source, created_at::DATE AS date_key, rating, title, content,
                    length(content) AS content_len, reviewer_hash, verified_purchase, has_photo, helpful_count,
                    size_purchased, color_purchased, language FROM {silver('review')}"""
        if has["customer"]:
            t["dim_customer"] = f"""SELECT customer_key, source, source_customer_id, person_id, age,
                    CASE WHEN age IS NULL THEN NULL WHEN age < 20 THEN '<20' WHEN age < 30 THEN '20-29' WHEN age < 40 THEN '30-39'
                         WHEN age < 50 THEN '40-49' WHEN age < 60 THEN '50-59' ELSE '60+' END AS age_band,
                    city, region, postal_code, country, is_active, club_status, newsletter_freq
                FROM {silver('customer')}"""
        if has["location"]:
            t["dim_location"] = f"""SELECT location_key, source, postal_prefix, city, region, country, lat, lng FROM {silver('location')}"""
        if has["order_item"]:
            t["fact_order_item"] = f"""SELECT oi.order_item_key, oi.order_key, oi.source, oi.order_date AS date_key, oi.customer_key, oi.product_key,
                    oi.seller_key, oi.line_no, oi.quantity, oi.unit_price, oi.unit_price * oi.quantity AS line_total, oi.freight,
                    oi.currency, oi.unit_price * fx.to_vnd AS unit_price_vnd, oi.sales_channel
                FROM {silver('order_item')} oi LEFT JOIN fx ON oi.currency = fx.currency"""
            con.execute("""CREATE TEMP TABLE pcat AS SELECT product_key, arg_max(category_l1, snapshot_date) AS category_l1,
                    arg_max(category_l2, snapshot_date) AS category_l2 FROM pv GROUP BY product_key""")
            t["agg_sales_month_category"] = f"""SELECT oi.source, date_trunc('month', oi.order_date)::DATE AS month,
                    coalesce(c.category_l1, 'unknown') AS category_l1, coalesce(c.category_l2, 'unknown') AS category_l2,
                    count(DISTINCT oi.order_key) AS n_orders, count(DISTINCT oi.customer_key) AS n_customers, sum(oi.quantity) AS units,
                    round(sum(oi.unit_price * oi.quantity), 2) AS revenue, round(sum(oi.unit_price * oi.quantity * fx.to_vnd)) AS revenue_vnd
                FROM {silver('order_item')} oi LEFT JOIN pcat c USING (product_key) LEFT JOIN fx ON oi.currency = fx.currency
                GROUP BY ALL"""
        if has["order"]:
            items = (f"LEFT JOIN (SELECT order_key, count(*) AS n_items, sum(quantity) AS units, sum(unit_price * quantity) AS items_total, "
                     f"sum(freight) AS freight_total FROM {silver('order_item')} GROUP BY order_key) i USING (order_key)"
                     if has["order_item"] else "")
            pays = (f"LEFT JOIN (SELECT order_key, sum(amount) AS paid_total, count(*) AS n_payments FROM {silver('payment')} "
                    f"GROUP BY order_key) pay USING (order_key)" if has["payment"] else "")
            cols = ("i.n_items, i.units, i.items_total, i.freight_total, i.items_total * fx.to_vnd AS items_total_vnd"
                    if has["order_item"] else "NULL AS n_items")
            t["fact_order"] = f"""SELECT o.order_key, o.source, o.customer_key, o.status, o.order_ts::DATE AS date_key, o.order_ts,
                    o.approved_ts, o.shipped_ts, o.delivered_ts, o.estimated_delivery_ts,
                    date_diff('day', o.order_ts, o.delivered_ts) AS delivery_days,
                    (o.delivered_ts > o.estimated_delivery_ts) AS delivered_late, o.sales_channel, o.currency,
                    {cols}{', pay.paid_total, pay.n_payments' if has['payment'] else ''}
                FROM {silver('order')} o {items} {pays} LEFT JOIN fx ON o.currency = fx.currency"""
        if has["payment"]:
            t["fact_payment"] = f"""SELECT pm.payment_key, pm.order_key, pm.source, pm.seq, pm.method, pm.installments, pm.amount,
                    pm.currency, pm.amount * fx.to_vnd AS amount_vnd FROM {silver('payment')} pm LEFT JOIN fx ON pm.currency = fx.currency"""
        t["agg_category_price"] = """SELECT source, category_key, any_value(category_leaf) AS category_leaf, any_value(category_l1) AS category_l1,
                    any_value(category_l2) AS category_l2, audience_std AS audience, snapshot_date AS date_key,
                    count(*) AS n_products, count(price_vnd) AS n_priced, round(avg(price_vnd)) AS avg_price_vnd,
                    round(median(price_vnd)) AS median_price_vnd, round(avg(discount_pct), 2) AS avg_discount_pct,
                    round(avg(in_stock::INT), 3) AS in_stock_rate, round(avg(rating_avg) FILTER (WHERE rating_count > 0), 2) AS avg_rating
                FROM pv GROUP BY source, category_key, audience_std, snapshot_date"""
        t["agg_brand_summary"] = """SELECT brand_key, mode(trim(brand_std)) AS brand_name, list(DISTINCT source) AS sources,
                    count(DISTINCT product_key) AS n_products, round(avg(price_vnd)) AS avg_price_vnd,
                    round(avg(discount_pct), 2) AS avg_discount_pct, round(avg(rating_avg) FILTER (WHERE rating_count > 0), 2) AS avg_rating,
                    sum(sold_count) AS total_sold, max(snapshot_date) AS last_seen
                FROM pv WHERE brand_std IS NOT NULL GROUP BY brand_key"""

        out = {}
        for name, sql in t.items():
            tmp, final = layer(f"_tmp_gold_{name}"), layer("gold", name)
            shutil.rmtree(tmp, ignore_errors=True)
            tmp.mkdir(parents=True)
            con.execute(f"COPY ({sql}) TO '{(tmp / 'data.parquet').as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
            swap_dir(tmp, final)
            out[name] = con.execute(f"SELECT count(*) FROM read_parquet('{(final / 'data.parquet').as_posix()}')").fetchone()[0]
            self.say(ctx, task, f"gold/{name}: {out[name]:,} dòng")
        con.close()
        self.catalog(list(out))
        return {"tables": len(out), "rows": out}

    @staticmethod
    def catalog(tables: list[str]) -> None:
        # File DuckDB gồm view trỏ vào parquet gold: mở một file là truy vấn được cả kho (duckdb gold.duckdb).
        db = layer("gold", "gold.duckdb")
        db.unlink(missing_ok=True)
        con = duckdb.connect(str(db))
        for t in tables:
            con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{(layer('gold', t) / 'data.parquet').as_posix()}')")
        # Embedding/phát hiện món đồ do src/search ghi (env riêng): giữ lại trong catalog khi gold dựng lại file này.
        for name, sql in feature_views().items():
            con.execute(f"CREATE VIEW {name} AS {sql}")
        con.close()
