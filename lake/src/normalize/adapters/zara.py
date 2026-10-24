from src.normalize.adapters.common import cat_row, clean, infer_audience, js, parse_ts, sha1, strip_html, to_float, to_int
from lib.config import airflow_settings

SOURCE = "zara"
SECTION = {"WOMAN": "women", "MAN": "men", "KID": "kids"}
IN_STOCK = {"in_stock", "low_on_stock", "back_soon"}


def components(payload: dict):
    for g in (payload or {}).get("productGroups") or []:
        for e in g.get("elements") or []:
            yield from e.get("commercialComponents") or []


def media_of(cc: dict, width: int) -> list[dict]:
    out, seen = [], set()
    groups = [(None, cc.get("xmedia") or [])] + [(c.get("name"), c.get("xmedia") or []) for c in (cc.get("detail") or {}).get("colors") or []]
    for color, items in groups:
        for m in items:
            kind, url = m.get("type"), m.get("url")
            if kind not in ("image", "video") or not url or m.get("name") in seen:
                continue
            seen.add(m.get("name"))
            out.append({"url": url.replace("{width}", str(width)), "type": kind, "color": clean(color),
                        "w": to_int(m.get("width")), "h": to_int(m.get("height"))})
    return out


def emit_tree(nodes: list, emit, parent=None, level: int = 1, trail: str = "", section=None) -> None:
    for c in nodes or []:
        sec = c.get("sectionName") or section
        path = f"{trail} > {c.get('name')}" if trail else str(c.get("name"))
        emit("category", cat_row("zara", c["id"], c.get("name"), parent, level, path, (c.get("seo") or {}).get("keyword")))
        emit_tree(c.get("subcategories"), emit, c["id"], level + 1, path, sec)


def normalize(b, day: str, emit) -> None:
    cfg = airflow_settings()
    for r in b.rows("categories"):
        emit_tree((r["payload"] or {}).get("categories"), emit)
    width, seen = cfg.media_zara_width, set()
    currency = "VND" if cfg.crawl_zara_store.startswith("vn") else None
    country, lang = cfg.crawl_zara_store.split("/")[0].upper(), cfg.crawl_zara_store.split("/")[-1]
    emit("seller", {"seller_key": "zara:zara", "seller_id": "zara", "name": "Zara", "is_official": True,
                    "url": f"https://www.zara.com/{cfg.crawl_zara_store}", "country": country})
    for r in b.rows("products"):
        crawl_ts, sec = parse_ts(r["crawl_ts"]), r["req"].get("section")
        for cc in components(r["payload"]):
            if cc.get("type") != "Product" or not cc.get("price") or cc["id"] in seen:
                continue
            seen.add(cc["id"])
            pk = f"zara:{cc['id']}"
            seo = cc.get("seo") or {}
            colors = (cc.get("detail") or {}).get("colors") or []
            media = media_of(cc, width)
            images = [m for m in media if m["type"] == "image"]
            price, old = to_float(cc.get("price")), to_float(cc.get("oldPrice"))
            fam = [x for x in (clean(cc.get("familyName")), clean(cc.get("subfamilyName"))) if x]
            emit("product", {
                "product_key": pk, "source": SOURCE, "source_product_id": str(cc["id"]), "crawl_ts": crawl_ts,
                "name": clean(cc.get("name")), "brand": "Zara", "description": strip_html(cc.get("description")),
                "category_path": " > ".join([sec.title()] + fam if sec else fam), "category_leaf": fam[-1] if fam else None,
                "audience": SECTION.get(sec) or infer_audience(sec), "product_type": clean(cc.get("kind")),
                "currency": currency, "price": price, "original_price": old if old and price and old > price else None,
                "discount_pct": to_float(cc.get("displayDiscountPercentage")),
                "in_stock": cc.get("availability") in IN_STOCK, "seller_id": "zara", "seller_name": "Zara",
                "url": f"https://www.zara.com/{cfg.crawl_zara_store}/{seo['keyword']}-p{seo['seoProductId']}.html" if seo.get("keyword") else None,
                "image_url": images[0]["url"] if images else None, "image_count": len(images), "variant_count": len(colors),
                "country": country, "language": lang,
                "attributes": js({"reference": cc.get("reference"), "kind": cc.get("kind"), "family": cc.get("familyName"),
                                  "subfamily": cc.get("subfamilyName"), "category_id": r["req"].get("category"),
                                  "colors": [c.get("name") for c in colors]}),
            })
            for c in colors:
                emit("variant", {"variant_key": f"{pk}:{c['id']}", "product_key": pk, "source_variant_id": str(c["id"]),
                                 "sku": c.get("reference"), "color": clean(c.get("name")), "price": to_float(c.get("price")),
                                 "currency": currency, "in_stock": c.get("availability") in IN_STOCK})
            for i, m in enumerate(media):
                emit("media", {"media_key": sha1(pk, m["url"]), "product_key": pk, "crawl_ts": crawl_ts,
                               "media_type": m["type"], "role": "main" if i == 0 else "gallery", "position": i,
                               "url": m["url"], "color": m["color"], "width": m["w"], "height": m["h"]})
