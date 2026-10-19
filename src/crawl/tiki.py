from __future__ import annotations

import gzip
import hashlib
import json
import math
import random
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

from src.common.config import get_logger, load_config, paths

log = get_logger("crawl.tiki")

API = "https://tiki.vn/api/v2"
SELLER_API = "https://api.tiki.vn/product-detail/v2/widgets/seller"
PAGE_SIZE = 40
PAGE_CAP = 50
STAGES = ["listing", "detail", "sellers", "reviews"]


class Blocked(RuntimeError):
    pass


class Client:

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": cfg["user_agent"], "Accept": "application/json"})
        # Listing bị WAF giới hạn chặt hơn detail (~25 request liên tiếp là bị chặn) nên có nhịp riêng.
        self.interval = {"default": cfg["min_interval"], "listing": cfg["listing_interval"]}
        self.last = {"default": 0.0, "listing": 0.0}
        self.blocks = 0
        self.n = 0

    def _wait(self, kind: str) -> None:
        since = time.monotonic() - max(self.last.values())
        gap = max(self.interval[kind] * random.uniform(1.0, 1.3) - (time.monotonic() - self.last[kind]),
                  self.cfg["min_interval"] - since)
        if gap > 0:
            time.sleep(gap)
        self.last[kind] = time.monotonic()

    def get(self, url: str, params: dict, kind: str = "default") -> dict | None:
        for attempt in range(5):
            self._wait(kind)
            self.n += 1
            try:
                r = self.s.get(url, params=params, timeout=30)
            except requests.RequestException as e:
                log.warning("  lỗi mạng (%s), thử lại", e.__class__.__name__)
                time.sleep(10 * (attempt + 1))
                continue
            if r.status_code == 404:
                return None
            # WAF trả trang challenge HTML với status 200 nên phải kiểm tra content-type.
            if r.status_code == 200 and r.headers.get("content-type", "").startswith("application/json"):
                return r.json()
            if r.status_code in (200, 403, 429):
                self.blocks += 1
                if self.blocks > self.cfg["max_blocks"]:
                    raise Blocked(f"bị chặn {self.blocks} lần, dừng — chạy lại sau sẽ tiếp tục từ chỗ dở")
                self.interval[kind] = min(self.interval[kind] * 1.5, 60.0)
                log.warning("  bị chặn (HTTP %s, %s) sau %d request, nghỉ %ds, nhịp %s → %.1fs",
                            r.status_code, r.headers.get("content-type"), self.n, self.cfg["block_wait"],
                            kind, self.interval[kind])
                time.sleep(self.cfg["block_wait"])
                continue
            log.warning("  HTTP %s cho %s, thử lại", r.status_code, url)
            time.sleep(10 * (attempt + 1))
        log.error("  bỏ qua %s %s sau 5 lần thử", url, params)
        return None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def append(path: Path, req: dict, payload) -> None:
    line = json.dumps({"_crawl_ts": _now(), "_req": req, "payload": payload}, ensure_ascii=False)
    # Mỗi dòng là một gzip member riêng: bị ngắt giữa chừng chỉ hỏng dòng cuối.
    with open(path, "ab") as f:
        f.write(gzip.compress(line.encode("utf-8") + b"\n"))


def read_lines(path: Path):
    if not path.exists():
        return
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    return
    except (EOFError, gzip.BadGzipFile):
        return


class State:

    def __init__(self, path: Path):
        self.path = path
        self.d = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        for k in ("detail", "reviews", "sellers"):
            self.d.setdefault(k, {})
        self.dirty = 0

    def __getitem__(self, k: str) -> dict:
        return self.d[k]

    def touch(self) -> None:
        self.dirty += 1
        if self.dirty >= 100:
            self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.d), encoding="utf-8")
        tmp.replace(self.path)
        self.dirty = 0


def _total(paging: dict) -> int:
    digits = "".join(ch for ch in str(paging.get("total_text", "")) if ch.isdigit())
    return int(digits) if digits else paging.get("total", 0)


def stage_listing(c: Client, out: Path, cfg: dict) -> None:
    f = out / "listing.jsonl.gz"
    done = {(x["_req"]["category"], x["_req"]["page"]) for x in read_lines(f)}

    def page(cat: int, p: int) -> dict | None:
        res = c.get(f"{API}/products", {"category": cat, "limit": PAGE_SIZE, "page": p}, "listing")
        if res is not None:
            append(f, {"category": cat, "page": p}, res)
            done.add((cat, p))
        return res

    def walk(cat: int, depth: int) -> None:
        first = None if (cat, 1) in done else page(cat, 1)
        if first is None and (cat, 1) not in done:
            return
        if first is None:
            first = next(x["payload"] for x in read_lines(f) if x["_req"] == {"category": cat, "page": 1})
        total = _total(first["paging"])
        if total > PAGE_SIZE * PAGE_CAP:
            kids = c.get(f"{API}/categories", {"parent_id": cat}, "listing")
            children = (kids or {}).get("data") or []
            if children:
                append(out / "categories.jsonl.gz", {"parent_id": cat}, kids)
                log.info("  %s cat %s: %s sp > cap, chia %d danh mục con", "  " * depth, cat, total, len(children))
                for ch in children:
                    walk(ch["id"], depth + 1)
                return
            log.warning("  cat %s: %s sp nhưng không có danh mục con, chỉ lấy được %d", cat, total,
                        PAGE_SIZE * PAGE_CAP)
        last = min(first["paging"].get("last_page", 1), PAGE_CAP)
        for p in range(2, last + 1):
            if (cat, p) not in done:
                page(cat, p)
        log.info("  %s cat %s: %s sp, %d trang", "  " * depth, cat, total, last)

    for root in cfg["roots"]:
        walk(root, 0)


def listing_products(out: Path) -> dict[int, dict]:
    prods = {}
    for x in read_lines(out / "listing.jsonl.gz"):
        for it in x["payload"].get("data") or []:
            prods[it["id"]] = it
    return prods


def _stale(last: str | None, days: int, today: date) -> bool:
    return last is None or date.fromisoformat(last) <= today - timedelta(days=days)


def _progress(stage: str, i: int, n: int, t0: float) -> None:
    if i % 200 == 0 or i == n:
        el = time.monotonic() - t0
        eta = el / i * (n - i)
        log.info("  %s %d/%d (%.2f req/s, còn ~%dm)", stage, i, n, i / el, eta / 60)


def stage_detail(c: Client, out: Path, cfg: dict, st: State, today: date, limit: int | None) -> None:
    prods = listing_products(out)
    todo = [p for pid, p in prods.items() if _stale(st["detail"].get(str(pid)), cfg["detail_refresh_days"], today)]
    todo = todo[:limit] if limit else todo
    log.info("  detail: %d/%d sản phẩm cần lấy", len(todo), len(prods))
    t0 = time.monotonic()
    for i, p in enumerate(todo, 1):
        req = {"id": p["id"], "spid": p.get("seller_product_id")}
        res = c.get(f"{API}/products/{p['id']}", {"platform": "web", "spid": req["spid"]})
        if res is not None:
            append(out / "detail.jsonl.gz", req, res)
            st["detail"][str(p["id"])] = today.isoformat()
            st.touch()
        _progress("detail", i, len(todo), t0)


def stage_sellers(c: Client, out: Path, cfg: dict, st: State, today: date, limit: int | None) -> None:
    ids = sorted({p["seller_id"] for p in listing_products(out).values() if p.get("seller_id")})
    todo = [s for s in ids if _stale(st["sellers"].get(str(s)), cfg["seller_refresh_days"], today)]
    todo = todo[:limit] if limit else todo
    log.info("  sellers: %d/%d shop cần lấy", len(todo), len(ids))
    t0 = time.monotonic()
    for i, sid in enumerate(todo, 1):
        res = c.get(SELLER_API, {"seller_id": sid})
        if res is not None:
            append(out / "sellers.jsonl.gz", {"seller_id": sid}, res)
            st["sellers"][str(sid)] = today.isoformat()
            st.touch()
        _progress("sellers", i, len(todo), t0)


def _hash_id(v) -> str | None:
    return hashlib.sha256(f"tiki:{v}".encode()).hexdigest()[:16] if v is not None else None


def scrub_review(r: dict) -> dict:
    # Không lưu tên/avatar người mua; id được băm để vẫn đếm được số review mỗi người.
    by = r.get("created_by") or {}
    r["created_by"] = {"id_hash": _hash_id(by.get("id")), "purchased": by.get("purchased"),
                       "purchased_at": by.get("purchased_at"), "created_time": by.get("created_time"),
                       "group_id": by.get("group_id")}
    r["customer_id"] = _hash_id(r.get("customer_id"))
    for cm in r.get("comments") or []:
        for k in ("fullname", "full_name", "name", "avatar_url", "avatar"):
            cm.pop(k, None)
        if "customer_id" in cm:
            cm["customer_id"] = _hash_id(cm["customer_id"])
    return r


def stage_reviews(c: Client, out: Path, cfg: dict, st: State, today: date, limit: int | None) -> None:
    prods = sorted((p for p in listing_products(out).values() if p.get("review_count")),
                   key=lambda p: -p["review_count"])[:cfg["review_products"]]
    todo = [p for p in prods if st["reviews"].get(str(p["id"])) != p["review_count"]]
    todo = todo[:limit] if limit else todo
    log.info("  reviews: %d/%d sản phẩm có review mới", len(todo), len(prods))
    t0 = time.monotonic()
    for i, p in enumerate(todo, 1):
        pages = min(cfg["review_pages"], math.ceil(p["review_count"] / 20))
        ok = True
        for pg in range(1, pages + 1):
            req = {"product_id": p["id"], "spid": p.get("seller_product_id"), "page": pg}
            res = c.get(f"{API}/reviews", {**req, "limit": 20, "sort": "id|desc", "include": "comments"})
            if res is None:
                ok = False
                break
            res["data"] = [scrub_review(r) for r in res.get("data") or []]
            append(out / "reviews.jsonl.gz", req, res)
            if pg >= (res.get("paging") or {}).get("last_page", 0):
                break
        if ok:
            st["reviews"][str(p["id"])] = p["review_count"]
            st.touch()
        _progress("reviews", i, len(todo), t0)


def run(stages: list[str] | None = None, limit: int | None = None, roots: list[int] | None = None) -> None:
    cfg = dict(load_config()["crawl"]["tiki"])
    if roots:
        cfg["roots"] = roots
    root = paths().raw / "tiki"
    today = datetime.now(timezone.utc).date()
    out = root / today.isoformat()
    out.mkdir(parents=True, exist_ok=True)
    st = State(root / "_state.json")
    c = Client(cfg)
    t0 = time.monotonic()
    try:
        for s in stages or STAGES:
            log.info("[tiki] %s → %s", s, out)
            if s == "listing":
                stage_listing(c, out, cfg)
            else:
                globals()[f"stage_{s}"](c, out, cfg, st, today, limit)
    finally:
        st.save()
        log.info("[tiki] %d request trong %.1f phút, bị chặn %d lần", c.n, (time.monotonic() - t0) / 60, c.blocks)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--stages", nargs="*", choices=STAGES)
    ap.add_argument("--limit", type=int, help="giới hạn số mục mỗi stage (chạy thử)")
    ap.add_argument("--roots", nargs="*", type=int, help="ghi đè danh mục gốc")
    a = ap.parse_args()
    run(a.stages, a.limit, a.roots)
