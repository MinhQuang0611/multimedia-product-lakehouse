import os

from src.bronze.agent import bronze_dir
from src.common.core import RAW
from src.common.schema import sql_select
from src.normalize.adapters.common import cat_row, clean, infer_audience, js, parse_ts, sha1, to_float, to_int

HM_AUDIENCE = {"Ladieswear": "women", "Divided": "women", "Menswear": "men", "Baby/Children": "kids", "Sport": "unisex"}
FILES = {"hm": "images", "shopee": "images"}


def local_files(source: str, folder: str) -> dict[str, str]:
    root = RAW / source / folder
    if not root.is_dir():
        return {}
    with os.scandir(root) as it:
        return {e.name: e.path for e in it if e.is_file()}


def parquet(source: str, dataset: str, day: str, extra: str = "") -> str:
    return f"read_parquet('{(bronze_dir(source, dataset, day) / '*.parquet').as_posix()}'{extra})"


def has(b, dataset: str) -> bool:
    return dataset in b.datasets()


# ---------------------------------------------------------------- H&M
def hm(b, day: str, emit) -> None:
    imgs = local_files("hm", "images")
    cats = set()
    for r in b.rows("articles"):
        aid = r["article_id"].zfill(10)
        pk = f"hm:{aid}"
        path = [clean(r.get(k)) for k in ("index_group_name", "product_group_name", "product_type_name")]
        ids = [r.get("index_group_no"), r.get("product_group_name"), r.get("product_type_no")]
        for i, (cid, name) in enumerate(zip(ids, path)):
            if name and (i, cid) not in cats:
                cats.add((i, cid))
                emit("category", cat_row("hm", f"{i + 1}-{cid}", name, f"{i}-{ids[i - 1]}" if i else None, i + 1,
                                         " > ".join(x for x in path[:i + 1] if x)))
        emit("product", {
            "product_key": pk, "source": "hm", "source_product_id": aid, "name": clean(r.get("prod_name")),
            "brand": "H&M", "description": clean(r.get("detail_desc")),
            "category_path": " > ".join(x for x in path if x) or None, "category_leaf": path[-1],
            "audience": HM_AUDIENCE.get(path[0]) or "unknown", "product_type": path[-1],
            "image_url": None, "image_count": int(f"{aid}.jpg" in imgs), "variant_count": 1, "language": "en",
            "attributes": js({"product_code": r.get("product_code"), "department": r.get("department_name"),
                              "section": r.get("section_name"), "garment_group": r.get("garment_group_name"),
                              "pattern": r.get("graphical_appearance_name"), "colour_master": r.get("perceived_colour_master_name")}),
        })
        emit("variant", {"variant_key": f"{pk}:{aid}", "product_key": pk, "source_variant_id": aid,
                         "color": clean(r.get("colour_group_name"))})
        if f"{aid}.jpg" in imgs:
            emit("media", {"media_key": sha1(pk, aid), "product_key": pk, "media_type": "image", "role": "main",
                           "position": 0, "source_path": imgs[f"{aid}.jpg"]})


def hm_sql(b, day: str) -> dict[str, str]:
    """Khách hàng và giao dịch H&M (31,8 triệu dòng) chuẩn hoá thẳng bằng SQL, không đi qua Python."""
    out = {}
    base = {"source": "'hm'", "snapshot_date": f"DATE '{day}'"}
    order_key = "'hm:' || left(md5(customer_id || '|' || t_dat || '|' || sales_channel_id), 20)"
    if has(b, "customers"):
        out["customer"] = sql_select("customer", {
            **base, "customer_key": "'hm:' || customer_id", "source_customer_id": "customer_id", "person_id": "customer_id",
            "age": "try_cast(try_cast(age AS DOUBLE) AS INTEGER)", "postal_code": "postal_code",
            "is_active": "CASE WHEN \"Active\" IS NULL THEN NULL ELSE try_cast(\"Active\" AS DOUBLE) = 1 END",
            "club_status": "club_member_status", "newsletter_freq": "fashion_news_frequency",
        }, parquet("hm", "customers", day))
    if has(b, "transactions"):
        tx = parquet("hm", "transactions", day, ", file_row_number=true")
        out["order_item"] = sql_select("order_item", {
            **base, "order_item_key": "'hm:' || file_row_number", "order_key": order_key,
            "customer_key": "'hm:' || customer_id", "product_key": "'hm:' || lpad(article_id, 10, '0')",
            "quantity": "1", "unit_price": "try_cast(price AS DOUBLE)", "order_date": "try_cast(t_dat AS DATE)",
            "sales_channel": "'channel_' || sales_channel_id",
        }, tx)
        out["order"] = sql_select("order", {
            **base, "order_key": order_key, "source_order_id": "left(md5(customer_id || '|' || t_dat || '|' || sales_channel_id), 20)",
            "customer_key": "'hm:' || customer_id", "order_ts": "CAST(t_dat AS TIMESTAMPTZ)",
            "sales_channel": "'channel_' || sales_channel_id",
        }, f"(SELECT customer_id, t_dat, sales_channel_id FROM {parquet('hm', 'transactions', day)} "
           f"GROUP BY customer_id, t_dat, sales_channel_id)")
    return out


# ---------------------------------------------------------------- Shopee
def shopee(b, day: str, emit) -> None:
    imgs = local_files("shopee", "images")
    for r in b.rows("train"):
        pid = r["posting_id"]
        pk = f"shopee:{pid}"
        emit("product", {
            "product_key": pk, "source": "shopee", "source_product_id": pid, "name": clean(r.get("title")),
            "audience": infer_audience(r.get("title")), "image_count": int(r["image"] in imgs), "country": "ID",
            "language": "id", "attributes": js({"label_group": r.get("label_group"), "image_phash": r.get("image_phash")}),
        })
        if r["image"] in imgs:
            emit("media", {"media_key": sha1(pk, r["image"]), "product_key": pk, "media_type": "image", "role": "main",
                           "position": 0, "source_path": imgs[r["image"]]})


# ---------------------------------------------------------------- Olist
def olist(b, day: str, emit) -> None:
    en = {r["product_category_name"]: r["product_category_name_english"] for r in b.rows("category_translation")}
    for pt, name in en.items():
        emit("category", cat_row("olist", pt, name, level=1, path=name))
    for r in b.rows("products"):
        pid, cat = r["product_id"], clean(r.get("product_category_name"))
        label = en.get(cat) or cat
        emit("product", {
            "product_key": f"olist:{pid}", "source": "olist", "source_product_id": pid, "category_path": label,
            "category_leaf": label, "audience": "unknown", "image_count": to_int(r.get("product_photos_qty")),
            "currency": "BRL", "country": "BR", "language": "pt",
            "attributes": js({"category_pt": cat, "weight_g": to_float(r.get("product_weight_g")),
                              "length_cm": to_float(r.get("product_length_cm")), "height_cm": to_float(r.get("product_height_cm")),
                              "width_cm": to_float(r.get("product_width_cm")),
                              "name_length": to_int(r.get("product_name_lenght")),
                              "description_length": to_int(r.get("product_description_lenght"))}),
        })

    # Mỗi customer_id trong Olist chỉ dùng cho một đơn; người mua thật là customer_unique_id.
    person: dict[str, str] = {}
    seen = set()
    for r in b.rows("customers"):
        uid = r["customer_unique_id"]
        person[r["customer_id"]] = f"olist:{uid}"
        if uid not in seen:
            seen.add(uid)
            emit("customer", {"customer_key": f"olist:{uid}", "source_customer_id": uid, "person_id": uid,
                              "city": clean(r.get("customer_city")), "region": r.get("customer_state"),
                              "postal_code": r.get("customer_zip_code_prefix"), "country": "BR"})
    dates: dict[str, str] = {}
    for r in b.rows("orders"):
        oid = r["order_id"]
        ts = parse_ts(r.get("order_purchase_timestamp"))
        dates[oid] = ts.date() if ts else None
        emit("order", {"order_key": f"olist:{oid}", "source_order_id": oid, "customer_key": person.get(r["customer_id"]),
                       "status": r.get("order_status"), "order_ts": ts, "approved_ts": parse_ts(r.get("order_approved_at")),
                       "shipped_ts": parse_ts(r.get("order_delivered_carrier_date")),
                       "delivered_ts": parse_ts(r.get("order_delivered_customer_date")),
                       "estimated_delivery_ts": parse_ts(r.get("order_estimated_delivery_date")), "currency": "BRL"})
    cust_of = {}
    for r in b.rows("orders"):
        cust_of[r["order_id"]] = person.get(r["customer_id"])
    for r in b.rows("order_items"):
        oid, n = r["order_id"], r["order_item_id"]
        emit("order_item", {"order_item_key": f"olist:{oid}:{n}", "order_key": f"olist:{oid}", "customer_key": cust_of.get(oid),
                            "product_key": f"olist:{r['product_id']}", "seller_key": f"olist:{r['seller_id']}",
                            "line_no": to_int(n), "quantity": 1, "unit_price": to_float(r.get("price")),
                            "freight": to_float(r.get("freight_value")), "currency": "BRL", "order_date": dates.get(oid)})
    for r in b.rows("payments"):
        oid, n = r["order_id"], r["payment_sequential"]
        emit("payment", {"payment_key": f"olist:{oid}:{n}", "order_key": f"olist:{oid}", "seq": to_int(n),
                         "method": r.get("payment_type"), "installments": to_int(r.get("payment_installments")),
                         "amount": to_float(r.get("payment_value")), "currency": "BRL"})
    for r in b.rows("reviews"):
        emit("review", {"review_key": f"olist:{r['review_id']}:{r['order_id']}", "order_key": f"olist:{r['order_id']}",
                        "rating": to_float(r.get("review_score")), "title": clean(r.get("review_comment_title")),
                        "content": clean(r.get("review_comment_message")), "created_at": parse_ts(r.get("review_creation_date")),
                        "language": "pt"})
    for r in b.rows("sellers"):
        emit("seller", {"seller_key": f"olist:{r['seller_id']}", "seller_id": r["seller_id"], "name": None,
                        "city": clean(r.get("seller_city")), "region": r.get("seller_state"),
                        "postal_prefix": r.get("seller_zip_code_prefix"), "country": "BR"})


def olist_sql(b, day: str) -> dict[str, str]:
    if not has(b, "geolocation"):
        return {}
    return {"location": sql_select("location", {
        "source": "'olist'", "snapshot_date": f"DATE '{day}'", "location_key": "'olist:' || geolocation_zip_code_prefix",
        "postal_prefix": "geolocation_zip_code_prefix", "city": "any_value(geolocation_city)",
        "region": "any_value(geolocation_state)", "country": "'BR'",
        "lat": "avg(try_cast(geolocation_lat AS DOUBLE))", "lng": "avg(try_cast(geolocation_lng AS DOUBLE))",
    }, f"{parquet('olist', 'geolocation', day)} GROUP BY geolocation_zip_code_prefix")}


def normalize(source: str):
    return {"hm": hm, "shopee": shopee, "olist": olist}[source]


SQL = {"hm": hm_sql, "olist": olist_sql}
