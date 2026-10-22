from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from src.common.config import get_logger, load_config
from src.crawl.common import Http, day_dir
from src.crawl.tiki import append, read_lines

log = get_logger("crawl.brands")

# Haravan, Sapo và Shopify cùng trả danh sách sản phẩm ở endpoint này; Haravan bỏ qua limit, luôn 50 sp/trang.
ENDPOINT = "https://{domain}/collections/all/products.json"
PAGE_CAP = 400


def crawl_site(site: str, conf: dict, out: Path, cfg: dict) -> tuple[str, int, int]:
    f = out / f"{site}.jsonl.gz"
    pages = {x["_req"]["page"]: len(x["payload"].get("products") or []) for x in read_lines(f)}
    n = sum(pages.values())
    if 0 in pages.values():
        return site, n, 0
    http = Http(cfg["min_interval"], cfg["user_agent"])
    page = max(pages, default=0) + 1
    while page <= PAGE_CAP:
        res = http.get_json(ENDPOINT.format(domain=conf["domain"]), {"limit": 250, "page": page})
        if res is None:
            log.warning("  %s: dừng ở trang %d, lần chạy sau tiếp tục", site, page)
            break
        # Trang rỗng cuối cùng cũng được ghi để lần chạy lại biết site này đã xong trong ngày.
        append(f, {"site": site, "domain": conf["domain"], "platform": conf["platform"], "page": page}, res)
        k = len(res.get("products") or [])
        if k == 0:
            break
        n += k
        page += 1
    return site, n, http.n


def run(sites: list[str] | None = None) -> None:
    cfg = load_config()["crawl"]["brands"]
    todo = {s: c for s, c in cfg["sites"].items() if not sites or s in sites}
    out, lock = day_dir("brands")
    if out is None:
        return
    t0 = time.monotonic()
    try:
        log.info("[brands] %d site → %s", len(todo), out)
        # Haravan/Sapo: mỗi site một host, chạy song song. Shopify giới hạn theo IP trên toàn hạ tầng của
        # họ (5 site song song → 429), nên mọi site Shopify chạy lần lượt trong một luồng, nhịp chậm hơn.
        shopify = [(s, c) for s, c in todo.items() if c["platform"] == "shopify"]
        others = [(s, c) for s, c in todo.items() if c["platform"] != "shopify"]
        slow = {**cfg, "min_interval": cfg["shopify_interval"]}
        with ThreadPoolExecutor(cfg["workers"]) as ex:
            futs = [ex.submit(crawl_site, s, c, out, cfg) for s, c in others]
            futs.append(ex.submit(lambda: [crawl_site(s, c, out, slow) for s, c in shopify]))
            for fu in futs:
                try:
                    res = fu.result()
                    for site, n, req in res if isinstance(res, list) else [res]:
                        log.info("  %-14s %6d sp, %3d request", site, n, req)
                except Exception as e:
                    log.error("  lỗi: %r", e)
    finally:
        lock.close()
        log.info("[brands] xong trong %.1f phút", (time.monotonic() - t0) / 60)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--sites", nargs="*", help="chỉ crawl các site này (khoá trong config)")
    run(ap.parse_args().sites)
