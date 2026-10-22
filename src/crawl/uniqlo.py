from __future__ import annotations

import math
import time
from datetime import date

from src.common.config import get_logger, load_config, paths
from src.crawl.common import Http, day_dir, done_reqs, req_key
from src.crawl.tiki import State, _stale, append, read_lines

log = get_logger("crawl.uniqlo")
PAGE = 100


def stage_listing(http: Http, base: str, out, cfg: dict) -> dict[str, tuple[str, dict]]:
    done = {}
    for g in cfg["genders"].values():
        for x in read_lines(out / "listing" / f"{g}.jsonl.gz"):
            done[req_key(x["_req"])] = x["payload"]
    for path, g in cfg["genders"].items():
        off, total = 0, None
        while total is None or off < total:
            req = {"path": path, "offset": off}
            res = done.get(req_key(req))
            if res is None:
                res = http.get_json(f"{base}/products", {**req, "limit": PAGE})
                if res is None or res.get("status") != "ok":
                    log.warning("  listing %s offset %d lỗi, bỏ qua phần còn lại", g, off)
                    break
                append(out / "listing" / f"{g}.jsonl.gz", req, res)
            total = res["result"]["pagination"]["total"]
            off += PAGE
    # Sản phẩm unisex có ở cả women lẫn men: giữ ở nhóm gặp trước theo thứ tự trong config.
    prods = {}
    for g in cfg["genders"].values():
        for x in read_lines(out / "listing" / f"{g}.jsonl.gz"):
            for it in x["payload"]["result"].get("items") or []:
                prods.setdefault(it["productId"], (g, it))
    return prods


def scrub_review(r: dict) -> dict:
    # Không lưu nickname người viết review.
    for k in ("name", "nickname", "userNickname"):
        r.pop(k, None)
    return r


def run(limit: int | None = None) -> None:
    cfg = load_config()["crawl"]["uniqlo"]
    base = f"https://www.uniqlo.com/{cfg['store']}/api/commerce/v5/{cfg['lang']}"
    out, lock = day_dir("uniqlo")
    if out is None:
        return
    today = date.today()
    st = State(paths().raw / "uniqlo" / "_state.json")
    http = Http(cfg["min_interval"], cfg["user_agent"])
    t0 = time.monotonic()
    try:
        prods = stage_listing(http, base, out, cfg)
        items = list(prods.items())[:limit] if limit else list(prods.items())
        log.info("[uniqlo] %d sản phẩm → %s", len(items), out)

        # Tồn kho + giá theo SKU lấy mỗi ngày; chi tiết 7 ngày/lần; review khi số review đổi.
        done = done_reqs((out / "stock").glob("*.jsonl.gz"))
        for i, (pid, (g, it)) in enumerate(items, 1):
            req = {"product_id": pid, "price_group": it["priceGroup"]}
            if req_key(req) not in done:
                res = http.get_json(f"{base}/products/{pid}/price-groups/{it['priceGroup']}/l2s",
                                    {"withPrices": "true", "withStocks": "true"})
                if res is not None:
                    append(out / "stock" / f"{g}.jsonl.gz", req, res)
            if _stale(st["detail"].get(pid), cfg["detail_refresh_days"], today):
                res = http.get_json(f"{base}/products/{pid}/price-groups/{it['priceGroup']}/details",
                                    {"includeModelSize": "true"})
                if res is not None:
                    append(out / "details" / f"{g}.jsonl.gz", req, res)
                    st["detail"][pid] = today.isoformat()
                    st.touch()
            count = (it.get("rating") or {}).get("count") or 0
            if count and st["reviews"].get(pid) != count:
                ok = True
                for pg in range(min(cfg["review_pages"], math.ceil(count / 20))):
                    res = http.get_json(f"{base}/products/{pid}/reviews",
                                        {"limit": 20, "offset": 20 * pg, "sort": "submission_time"})
                    if res is None or res.get("status") != "ok":
                        ok = False
                        break
                    res["result"]["reviews"] = [scrub_review(r) for r in res["result"].get("reviews") or []]
                    append(out / "reviews" / f"{g}.jsonl.gz", {"product_id": pid, "offset": 20 * pg}, res)
                if ok:
                    st["reviews"][pid] = count
                    st.touch()
            if i % 200 == 0 or i == len(items):
                el = time.monotonic() - t0
                log.info("  %d/%d sản phẩm, %d request (%.2f req/s)", i, len(items), http.n, http.n / el)
    finally:
        st.save()
        lock.close()
        log.info("[uniqlo] %d request trong %.1f phút", http.n, (time.monotonic() - t0) / 60)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="chỉ lấy N sản phẩm đầu (chạy thử)")
    run(ap.parse_args().limit)
