import pyarrow as pa

TS = pa.timestamp("us", tz="UTC")
STR, F64, I64, I32, BOOL, DATE = pa.string(), pa.float64(), pa.int64(), pa.int32(), pa.bool_(), pa.date32()

# Đổi cấu trúc cột thì tăng số này: normalize sẽ dựng lại mọi ngày thay vì dùng bản cũ.
SCHEMA_VERSION = 2

# Schema chuẩn duy nhất cho mọi nguồn (Tiki, web hãng, Zara, Uniqlo, H&M, Shopee, Olist).
# Cột đặc thù của từng nguồn không có chỗ riêng thì nằm trong `attributes` (JSON).
# Mọi entity đều có `source` và `snapshot_date`; khoá là `<nguồn>:<id gốc>`.
ENTITIES: dict[str, list[tuple[str, pa.DataType]]] = {
    "product": [
        ("product_key", STR), ("source", STR), ("source_product_id", STR), ("snapshot_date", DATE), ("crawl_ts", TS),
        ("name", STR), ("brand", STR), ("brand_std", STR), ("description", STR), ("category_path", STR),
        ("category_leaf", STR), ("category_l1", STR), ("category_l2", STR), ("audience", STR), ("product_type", STR),
        ("currency", STR), ("price", F64), ("original_price", F64), ("discount_pct", F64), ("in_stock", BOOL),
        ("stock_qty", I64), ("sold_count", I64), ("rating_avg", F64), ("rating_count", I64), ("seller_id", STR),
        ("seller_name", STR), ("url", STR), ("image_url", STR), ("image_count", I32), ("variant_count", I32),
        ("tags", STR), ("country", STR), ("language", STR), ("source_created_at", TS), ("source_updated_at", TS),
        ("attributes", STR),
    ],
    "variant": [
        ("variant_key", STR), ("product_key", STR), ("source", STR), ("source_variant_id", STR),
        ("snapshot_date", DATE), ("sku", STR), ("color", STR), ("size", STR), ("price", F64),
        ("original_price", F64), ("currency", STR), ("in_stock", BOOL), ("stock_qty", I64),
    ],
    "media": [
        ("media_key", STR), ("product_key", STR), ("source", STR), ("snapshot_date", DATE), ("media_type", STR),
        ("role", STR), ("position", I32), ("url", STR), ("source_path", STR), ("color", STR), ("width", I32),
        ("height", I32),
    ],
    # Đánh giá: gắn với sản phẩm (Tiki, Uniqlo) hoặc với đơn hàng (Olist chỉ cho điểm theo đơn).
    "review": [
        ("review_key", STR), ("product_key", STR), ("order_key", STR), ("source", STR), ("snapshot_date", DATE),
        ("rating", F64), ("title", STR), ("content", STR), ("created_at", TS), ("reviewer_hash", STR),
        ("verified_purchase", BOOL), ("has_photo", BOOL), ("helpful_count", I64), ("size_purchased", STR),
        ("color_purchased", STR), ("language", STR),
    ],
    "seller": [
        ("seller_key", STR), ("source", STR), ("seller_id", STR), ("snapshot_date", DATE), ("name", STR),
        ("rating", F64), ("review_count", I64), ("follower_count", I64), ("is_official", BOOL), ("url", STR),
        ("country", STR), ("city", STR), ("region", STR), ("postal_prefix", STR),
    ],
    # Cây danh mục gốc của từng nguồn (không phải danh mục chuẩn: danh mục chuẩn là product.category_l1/l2).
    "category": [
        ("category_key", STR), ("source", STR), ("source_category_id", STR), ("snapshot_date", DATE),
        ("parent_key", STR), ("name", STR), ("level", I32), ("path", STR), ("n_products", I64), ("url", STR),
    ],
    "customer": [
        ("customer_key", STR), ("source", STR), ("source_customer_id", STR), ("snapshot_date", DATE),
        ("person_id", STR), ("age", I32), ("city", STR), ("region", STR), ("postal_code", STR), ("country", STR),
        ("is_active", BOOL), ("club_status", STR), ("newsletter_freq", STR),
    ],
    "order": [
        ("order_key", STR), ("source", STR), ("source_order_id", STR), ("snapshot_date", DATE), ("customer_key", STR),
        ("status", STR), ("order_ts", TS), ("approved_ts", TS), ("shipped_ts", TS), ("delivered_ts", TS),
        ("estimated_delivery_ts", TS), ("sales_channel", STR), ("currency", STR),
    ],
    "order_item": [
        ("order_item_key", STR), ("order_key", STR), ("source", STR), ("snapshot_date", DATE), ("customer_key", STR),
        ("product_key", STR), ("seller_key", STR), ("line_no", I32), ("quantity", I32), ("unit_price", F64),
        ("freight", F64), ("currency", STR), ("order_date", DATE), ("sales_channel", STR),
    ],
    "payment": [
        ("payment_key", STR), ("order_key", STR), ("source", STR), ("snapshot_date", DATE), ("seq", I32),
        ("method", STR), ("installments", I32), ("amount", F64), ("currency", STR),
    ],
    "location": [
        ("location_key", STR), ("source", STR), ("snapshot_date", DATE), ("postal_prefix", STR), ("city", STR),
        ("region", STR), ("country", STR), ("lat", F64), ("lng", F64),
    ],
}

DUCK_TYPES = {STR: "VARCHAR", F64: "DOUBLE", I64: "BIGINT", I32: "INTEGER", BOOL: "BOOLEAN", DATE: "DATE",
              TS: "TIMESTAMPTZ"}


def arrow_schema(entity: str) -> pa.Schema:
    return pa.schema(ENTITIES[entity])


def to_table(entity: str, rows: list[dict]) -> pa.Table:
    schema = arrow_schema(entity)
    cols = {name: [r.get(name) for r in rows] for name in schema.names}
    return pa.table(cols, schema=schema)


def sql_select(entity: str, exprs: dict[str, str], from_sql: str) -> str:
    """SELECT đủ cột của entity (đúng thứ tự và kiểu); cột không khai báo biểu thức thì NULL."""
    cols = [f"CAST({exprs.get(name, 'NULL')} AS {DUCK_TYPES[typ]}) AS \"{name}\"" for name, typ in ENTITIES[entity]]
    return f"SELECT {', '.join(cols)} FROM {from_sql}"
