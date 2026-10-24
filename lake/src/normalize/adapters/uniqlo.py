from src.normalize.adapters.common import cat_row, clean, infer_audience, js, parse_ts, sha1, strip_html, to_float, to_int
from lib.config import airflow_settings

SOURCE = "uniqlo"
GENDER = {"WOMEN": "women", "MEN": "men", "KIDS": "kids", "BABY": "kids", "UNISEX": "unisex"}


def by_product(b, dataset: str) -> dict[str, dict]:
    out = {}
    for r in b.rows(dataset):
        res = (r["payload"] or {}).get("result")
        if isinstance(res, dict):
            out[r["req"]["product_id"]] = res
    return out


def image_list(images: dict, colors: dict[str, str]) -> list[dict]:
    out = []
    for code, m in (images.get("main") or {}).items():
        if m.get("image"):
            out.append({"url": m["image"], "color": colors.get(code)})
    for m in images.get("sub") or []:
        if m.get("image"):
            out.append({"url": m["image"], "color": colors.get(m.get("colorCode"))})
    return list({x["url"]: x for x in out}.values())


def normalize(b, day: str, emit) -> None:
    store, lang = airflow_settings().crawl_uniqlo_store, airflow_settings().crawl_uniqlo_lang
    details, stocks = by_product(b, "details"), by_product(b, "stock")
    country = store.upper()
    emit("seller", {"seller_key": "uniqlo:uniqlo", "seller_id": "uniqlo", "name": "Uniqlo", "is_official": True,
                    "url": f"https://www.uniqlo.com/{store}/{lang}", "country": country})
    seen, cats = set(), set()
    for r in b.rows("listing"):
        crawl_ts = parse_ts(r["crawl_ts"])
        for it in ((r["payload"] or {}).get("result") or {}).get("items") or []:
            pid = it["productId"]
            if pid in seen:
                continue
            seen.add(pid)
            pk = f"uniqlo:{pid}"
            d, st = details.get(pid) or {}, stocks.get(pid) or {}
            colors = {c["displayCode"]: clean(c.get("name")) for c in it.get("colors") or []}
            sizes = {s["displayCode"]: clean(s.get("name")) for s in it.get("sizes") or []}
            prices = it.get("prices") or {}
            price = to_float((prices.get("promo") or prices.get("base") or {}).get("value"))
            base = to_float((prices.get("base") or {}).get("value"))
            currency = ((prices.get("base") or {}).get("currency") or {}).get("code")
            chain = sorted((c for c in (d.get("breadcrumbs") or {}).values() if isinstance(c, dict)), key=lambda c: c.get("level") or 0)
            crumbs = [c.get("locale") or c.get("name") for c in chain if c.get("locale") or c.get("name")]
            for i, c in enumerate(chain):
                if c.get("id") and c["id"] not in cats:
                    cats.add(c["id"])
                    emit("category", cat_row("uniqlo", c["id"], c.get("locale") or c.get("name"), chain[i - 1].get("id") if i else None,
                                             i + 1, " > ".join(crumbs[:i + 1])))
            gender = GENDER.get(str(it.get("genderCategory") or "").upper()) or infer_audience(it.get("genderName"))
            variants = []
            for l2 in st.get("l2s") or []:
                s = (st.get("stocks") or {}).get(l2["l2Id"]) or {}
                px = (st.get("prices") or {}).get(l2["l2Id"]) or {}
                vp = to_float(((px.get("promo") or px.get("base")) or {}).get("value")) if isinstance(px, dict) else None
                variants.append({"variant_key": f"{pk}:{l2['l2Id']}", "product_key": pk, "source_variant_id": l2["l2Id"],
                                 "sku": l2.get("communicationCode"), "color": colors.get(l2["color"]["displayCode"]),
                                 "size": sizes.get(l2["size"]["displayCode"]), "price": vp or price, "currency": currency,
                                 "original_price": base if base and price and base > price else None,
                                 "in_stock": s.get("statusCode") == "IN_STOCK" or (s.get("quantity") or 0) > 0,
                                 "stock_qty": to_int(s.get("quantity"))})
            qtys = [v["stock_qty"] for v in variants if v["stock_qty"] is not None]
            media = image_list(d.get("images") or it.get("images") or {}, colors)
            rating = it.get("rating") or {}
            emit("product", {
                "product_key": pk, "source": SOURCE, "source_product_id": pid, "crawl_ts": crawl_ts,
                "name": clean(it.get("name")), "brand": "Uniqlo",
                "description": strip_html(d.get("longDescription") or d.get("shortDescription")),
                "category_path": " > ".join(crumbs) or None, "category_leaf": crumbs[-1] if crumbs else None,
                "audience": gender, "product_type": clean(d.get("productType")) if d.get("productType") != "unknown" else None,
                "currency": currency, "price": price, "original_price": base if base and price and base > price else None,
                "in_stock": any(v["in_stock"] for v in variants) if variants else None,
                "stock_qty": sum(qtys) if qtys else None, "rating_avg": to_float(rating.get("average")),
                "rating_count": to_int(rating.get("count")), "seller_id": "uniqlo", "seller_name": "Uniqlo",
                "url": f"https://www.uniqlo.com/{store}/{lang}/products/{pid}/{it.get('priceGroup', '00')}",
                "image_url": media[0]["url"] if media else None, "image_count": len(media),
                "variant_count": len(variants), "country": country, "language": lang,
                "attributes": js({"composition": clean(d.get("composition")), "care": clean(d.get("washingInformation")),
                                  "design_detail": strip_html(d.get("designDetail")),
                                  "origin": d.get("countriesOfOrigin"), "recycled": d.get("recycledMaterial"),
                                  "best_seller": d.get("isBestSeller"), "stock_capped_at": 11 if qtys else None}),
            })
            for v in variants:
                emit("variant", v)
            for i, m in enumerate(media):
                emit("media", {"media_key": sha1(pk, m["url"]), "product_key": pk, "crawl_ts": crawl_ts, "media_type": "image",
                               "role": "main" if i == 0 else "gallery", "position": i, "url": m["url"], "color": m["color"]})

    for r in b.rows("reviews"):
        pid = r["req"]["product_id"]
        for rv in ((r["payload"] or {}).get("result") or {}).get("reviews") or []:
            emit("review", {"review_key": f"uniqlo:{rv.get('reviewId') or rv.get('bvId')}", "product_key": f"uniqlo:{pid}",
                            "rating": to_float(rv.get("rate")), "title": clean(rv.get("title")),
                            "content": clean(rv.get("comment")), "created_at": parse_ts(rv.get("createDate")),
                            "helpful_count": to_int(rv.get("helpfulCount")), "size_purchased": clean(rv.get("purchasedSize")),
                            "color_purchased": clean(rv.get("purchasedColorName")), "language": lang})
