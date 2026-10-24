import re

from src.normalize.adapters.common import cat_row, clean, infer_audience, js, option_role, parse_ts, sha1, strip_html, to_float, \
    to_int

SOURCE = "tiki"
BASE = "https://tiki.vn/"


def big(url: str | None) -> str | None:
    # Ảnh listing là bản cache 280x280; đổi sang bản w1200 như ảnh trong detail.
    return re.sub(r"/cache/\d+x\d+/", "/cache/w1200/", url) if url else url


def normalize(b, day: str, emit) -> None:
    details = {}
    for r in b.rows("detail"):
        p = r["payload"]
        if isinstance(p, dict) and p.get("id"):
            details[p["id"]] = p
    seen, cats = set(), set()
    for r in b.rows("listing"):
        crawl_ts = parse_ts(r["crawl_ts"])
        for it in (r["payload"] or {}).get("data") or []:
            pid = it["id"]
            if pid in seen:
                continue
            seen.add(pid)
            d = details.get(pid) or {}
            pk = f"tiki:{pid}"
            chain = [c for c in d.get("breadcrumbs") or [] if c.get("category_id")]
            crumbs = [c["name"] for c in chain]
            for i, c in enumerate(chain):
                if c["category_id"] not in cats:
                    cats.add(c["category_id"])
                    emit("category", cat_row("tiki", c["category_id"], c["name"], chain[i - 1]["category_id"] if i else None,
                                             i + 1, " > ".join(crumbs[:i + 1]), BASE + c["url"].lstrip("/") if c.get("url") else None))
            # Một số danh mục lá trả tên "Root": lấy tên danh mục sâu nhất trong breadcrumbs.
            leaf = (d.get("categories") or {}).get("name") or it.get("primary_category_name")
            if leaf == "Root":
                leaf = crumbs[-1] if crumbs else None
            path = " > ".join(crumbs) if crumbs else leaf
            imgs = [im.get("large_url") or im.get("base_url") for im in d.get("images") or []]
            imgs = [u for u in dict.fromkeys(imgs) if u] or [big(it.get("thumbnail_url"))]
            imgs = [u for u in imgs if u]
            variants = d.get("configurable_products") or []
            opts = {o["code"]: option_role(o.get("name")) for o in d.get("configurable_options") or []}
            specs = {a.get("name"): a.get("value") for g in d.get("specifications") or []
                     for a in g.get("attributes") or []}
            emit("product", {
                "product_key": pk, "source": SOURCE, "source_product_id": str(pid), "crawl_ts": crawl_ts,
                "name": clean(it.get("name")), "brand": clean(it.get("brand_name") or (d.get("brand") or {}).get("name")),
                "description": strip_html(d.get("description")) or clean(d.get("short_description")),
                "category_path": path, "category_leaf": leaf,
                "audience": infer_audience(path, leaf, it.get("name")), "product_type": leaf, "currency": "VND",
                "price": to_float(it.get("price")), "original_price": to_float(it.get("original_price")),
                "discount_pct": to_float(it.get("discount_rate")),
                "in_stock": (d.get("inventory_status") == "available") if d else it.get("availability") == 1,
                "stock_qty": to_int((d.get("stock_item") or {}).get("qty")),
                "sold_count": to_int((it.get("quantity_sold") or {}).get("value")),
                "rating_avg": to_float(it.get("rating_average")), "rating_count": to_int(it.get("review_count")),
                "seller_id": str(it["seller_id"]) if it.get("seller_id") else None,
                "seller_name": clean(it.get("seller_name")), "url": BASE + it["url_path"] if it.get("url_path") else None,
                "image_url": imgs[0] if imgs else None, "image_count": len(imgs), "variant_count": len(variants),
                "country": "VN", "language": "vi",
                "attributes": js({"specifications": specs, "origin": it.get("origin"), "brand_id": it.get("brand_id"),
                                  "official_store": it.get("is_from_official_store"),
                                  "authentic": it.get("is_authentic"), "fulfillment": (d.get("inventory") or {}).get(
                                      "fulfillment_type"), "age_days": d.get("day_ago_created")}),
            })
            for i, u in enumerate(imgs):
                emit("media", {"media_key": sha1(pk, u), "product_key": pk, "crawl_ts": crawl_ts, "media_type": "image",
                               "role": "main" if i == 0 else "gallery", "position": i, "url": u})
            for cp in variants:
                color = size = None
                for code, role in opts.items():
                    v = clean(cp.get(code))
                    color, size = (v, size) if role == "color" else (color, v) if role == "size" else (color, size)
                emit("variant", {"variant_key": f"{pk}:{cp['id']}", "product_key": pk, "source_variant_id": str(cp["id"]),
                                 "sku": cp.get("sku"), "color": color, "size": size, "price": to_float(cp.get("price")),
                                 "original_price": to_float(cp.get("original_price")), "currency": "VND",
                                 "in_stock": cp.get("inventory_status") == "available"})

    for r in b.rows("reviews"):
        for rv in (r["payload"] or {}).get("data") or []:
            pid = rv.get("product_id") or r["req"].get("product_id")
            color = size = None
            for a in rv.get("product_attributes") or []:
                k, _, v = a.partition(":")
                role = option_role(k)
                color, size = (clean(v), size) if role == "color" else (color, clean(v)) if role == "size" else (color, size)
            by = rv.get("created_by") or {}
            emit("review", {"review_key": f"tiki:{rv['id']}", "product_key": f"tiki:{pid}", "rating": to_float(rv.get("rating")),
                            "title": clean(rv.get("title")), "content": clean(rv.get("content")),
                            "created_at": parse_ts(rv.get("created_at")), "reviewer_hash": rv.get("customer_id") or by.get("id_hash"),
                            "verified_purchase": by.get("purchased"), "has_photo": bool(rv.get("images")),
                            "helpful_count": to_int(rv.get("thank_count")), "size_purchased": size,
                            "color_purchased": color, "language": "vi"})

    for r in b.rows("sellers"):
        s = ((r["payload"] or {}).get("data") or {}).get("seller") or {}
        if s.get("id"):
            emit("seller", {"seller_key": f"tiki:{s['id']}", "seller_id": str(s["id"]), "name": clean(s.get("name")),
                            "rating": to_float(s.get("avg_rating_point")), "review_count": to_int(s.get("review_count")),
                            "follower_count": to_int(s.get("total_follower")), "is_official": s.get("is_official"),
                            "url": s.get("url"), "country": "VN"})
