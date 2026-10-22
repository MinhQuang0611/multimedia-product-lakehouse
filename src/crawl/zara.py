from __future__ import annotations

import time

from src.common.config import get_logger, load_config
from src.crawl.common import Http, day_dir
from src.crawl.tiki import append, node_name, read_lines

log = get_logger("crawl.zara")


def product_categories(tree: dict, sections: list[str]):
    # Chỉ lấy danh mục lá hiển thị lưới sản phẩm; danh mục marketing/divider không có sản phẩm.
    def walk(cs, section):
        for c in cs:
            sec = c.get("sectionName") or section
            subs = c.get("subcategories") or []
            if subs:
                yield from walk(subs, sec)
            elif (sec in sections and not c.get("irrelevant")
                  and str(c.get("layout") or "").endswith("products-category-view")):
                yield c, sec

    return list(walk(tree.get("categories") or [], None))


def run(limit: int | None = None) -> None:
    cfg = load_config()["crawl"]["zara"]
    base = f"https://www.zara.com/{cfg['store']}"
    out, lock = day_dir("zara")
    if out is None:
        return
    http = Http(cfg["min_interval"], cfg["user_agent"])
    t0 = time.monotonic()
    try:
        cat_file = out / "categories.jsonl.gz"
        tree = next((x["payload"] for x in read_lines(cat_file)), None)
        if tree is None:
            tree = http.get_json(f"{base}/categories", {"ajax": "true"})
            if tree is None:
                log.error("[zara] không lấy được cây danh mục")
                return
            append(cat_file, {"store": cfg["store"]}, tree)
        cats = product_categories(tree, cfg["sections"])
        cats = cats[:limit] if limit else cats
        log.info("[zara] %d danh mục sản phẩm → %s", len(cats), out)
        for i, (c, sec) in enumerate(cats, 1):
            slug = (c.get("seo") or {}).get("keyword") or c.get("key")
            f = out / "products" / sec.lower() / f"{node_name(c['id'], slug)}.jsonl.gz"
            if next(read_lines(f), None) is None:
                res = http.get_json(f"{base}/category/{c['id']}/products", {"ajax": "true"})
                if res is not None:
                    append(f, {"store": cfg["store"], "category": c["id"], "section": sec}, res)
            if i % 100 == 0 or i == len(cats):
                log.info("  %d/%d danh mục, %d request", i, len(cats), http.n)
    finally:
        lock.close()
        log.info("[zara] %d request trong %.1f phút", http.n, (time.monotonic() - t0) / 60)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="chỉ lấy N danh mục đầu (chạy thử)")
    run(ap.parse_args().limit)
