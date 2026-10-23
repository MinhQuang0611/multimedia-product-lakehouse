from src.normalize.adapters.common import cat_row, clean, infer_audience, js, option_role, parse_ts, sha1, strip_html, to_float, \
    to_int
from lib.config import airflow_settings

SOURCE = "brands"


def tags_of(v) -> str | None:
    items = v if isinstance(v, list) else str(v or "").split(",")
    return "|".join(t for t in (clean(x) for x in items) if t) or None


def images_of(p: dict) -> list[dict]:
    out = []
    for i, im in enumerate(p.get("images") or []):
        if isinstance(im, str):
            im = {"src": im}
        if im.get("src"):
            out.append({"url": im["src"], "w": im.get("width"), "h": im.get("height"), "pos": im.get("position") or i + 1})
    if not out and p.get("featured_image"):
        out.append({"url": p["featured_image"], "pos": 1})
    return sorted({x["url"]: x for x in out}.values(), key=lambda x: x["pos"])


def normalize(b, day: str, emit) -> None:
    sites = airflow_settings().crawl_brands_sites
    seen, sellers, cats = set(), set(), set()
    for r in b.rows("products"):
        req, crawl_ts = r["req"], parse_ts(r["crawl_ts"])
        site, dom, platform = req["site"], req["domain"], req["platform"]
        currency = sites.get(site, {}).get("currency", "VND")
        country, lang = ("VN", "vi") if currency == "VND" else ("US", "en")
        if site not in sellers:
            sellers.add(site)
            emit("seller", {"seller_key": f"brands:{site}", "seller_id": site, "name": site, "is_official": True,
                            "url": f"https://{dom}", "country": country})
        for p in (r["payload"] or {}).get("products") or []:
            pid = p["id"]
            if (site, pid) in seen:
                continue
            seen.add((site, pid))
            pk = f"brands:{site}/{pid}"
            name = clean(p.get("title") or p.get("name"))
            handle = p.get("handle") or p.get("alias")
            roles = [option_role(o.get("name")) for o in p.get("options") or []]
            variants = []
            for v in p.get("variants") or []:
                vals = [v.get("option1"), v.get("option2"), v.get("option3")]
                color = next((clean(x) for x, ro in zip(vals, roles) if ro == "color"), None)
                size = next((clean(x) for x, ro in zip(vals, roles) if ro == "size"), None)
                price, cmp = to_float(v.get("price")), to_float(v.get("compare_at_price"))
                variants.append({"variant_key": f"{pk}:{v['id']}", "product_key": pk, "source_variant_id": str(v["id"]),
                                 "sku": clean(v.get("sku")), "color": color, "size": size, "price": price,
                                 "original_price": cmp if cmp and price and cmp > price else None, "currency": currency,
                                 "in_stock": v.get("available"), "stock_qty": to_int(v.get("inventory_quantity"))})
            priced = [v for v in variants if v["price"]]
            low = min(priced, key=lambda v: v["price"]) if priced else {}
            qtys = [v["stock_qty"] for v in variants if v["stock_qty"] is not None]
            flags = [v["in_stock"] for v in variants if v["in_stock"] is not None]
            imgs = images_of(p)
            ptype = clean(p.get("product_type"))
            if ptype and (site, ptype) not in cats:
                cats.add((site, ptype))
                emit("category", cat_row("brands", f"{site}/{ptype}", ptype, level=1, path=ptype))
            url = f"https://{dom}{p['url']}" if str(p.get("url", "")).startswith("/") else f"https://{dom}/products/{handle}"
            emit("product", {
                "product_key": pk, "source": SOURCE, "source_product_id": f"{site}/{pid}", "crawl_ts": crawl_ts,
                "name": name, "brand": clean(p.get("vendor")) or site,
                "description": strip_html(p.get("body_html") or p.get("content") or p.get("summary")),
                "category_path": ptype, "category_leaf": ptype,
                "audience": infer_audience(ptype, name, tags_of(p.get("tags")), handle), "product_type": ptype,
                "currency": currency, "price": low.get("price"), "original_price": low.get("original_price"),
                "in_stock": any(flags) if flags else p.get("available"), "stock_qty": sum(qtys) if qtys else None,
                "sold_count": to_int(p.get("sole_quantity")), "seller_id": site, "seller_name": dom, "url": url,
                "image_url": imgs[0]["url"] if imgs else None, "image_count": len(imgs), "variant_count": len(variants),
                "tags": tags_of(p.get("tags")), "country": country, "language": lang,
                "source_created_at": parse_ts(p.get("created_at")), "source_updated_at": parse_ts(p.get("updated_at")),
                "attributes": js({"site": site, "platform": platform, "handle": handle, "published_at": p.get("published_at")}),
            })
            for v in variants:
                emit("variant", v)
            for i, im in enumerate(imgs):
                emit("media", {"media_key": sha1(pk, im["url"]), "product_key": pk, "crawl_ts": crawl_ts,
                               "media_type": "image", "role": "main" if i == 0 else "gallery", "position": i,
                               "url": im["url"], "width": to_int(im["w"]), "height": to_int(im["h"])})
