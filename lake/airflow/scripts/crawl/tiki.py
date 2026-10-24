import gzip
import hashlib
import json
import math
import os
import random
import signal
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from lib.config import get_logger, load_config, paths

log = get_logger("crawl.tiki")

API = "https://tiki.vn/api/v2"
SELLER_API = "https://api.tiki.vn/product-detail/v2/widgets/seller"
PAGE_SIZE = 40  # Tiki API listing page size.
PAGE_CAP = 50
STAGES = ["listing", "detail", "images", "sellers", "reviews"]
# Ba loại này chia file theo cây danh mục: <ngày>/<loại>/<gốc>/<con>/.../<danh mục>.jsonl.gz.
# sellers, categories nhỏ và không gắn với một danh mục nên vẫn một file mỗi ngày.
SPLIT_KINDS = ["listing", "detail", "reviews"]


class Blocked(RuntimeError):
    pass

class Client:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": cfg["user_agent"], "Accept": "application/json"})
        self.interval = {"default": cfg["min_interval"], "listing": cfg["listing_interval"]}
        self.last = {"default": 0.0, "listing": 0.0}
        self.burst = cfg["listing_burst"]
        self.listing_used = 0
        self.idle = None
        self.blocks = 0
        self.streak = 0
        self.n = 0

    def _rest(self, secs: float, why: str) -> None:
        log.info("  %s, nghỉ listing %d phút%s", why, secs / 60, " (lấy detail trong lúc chờ)" if self.idle else "")
        end = time.monotonic() + secs
        if self.idle:
            self.idle(end)
        if end > time.monotonic():
            time.sleep(end - time.monotonic())
        self.listing_used = 0

    def _wait(self, kind: str) -> None:
        since = time.monotonic() - max(self.last.values())
        gap = max(self.interval[kind] * random.uniform(1.0, 1.3) - (time.monotonic() - self.last[kind]),
                  self.cfg["min_interval"] - since)
        if gap > 0:
            time.sleep(gap)
        self.last[kind] = time.monotonic()

    def get(self, url: str, params: dict, kind: str = "default") -> dict | None:
        for attempt in range(5):
            if kind == "listing" and self.listing_used >= self.burst:
                self._rest(self.cfg["listing_pause"], f"đã dùng {self.listing_used} request listing")
            self._wait(kind)
            self.n += 1
            if kind == "listing":
                self.listing_used += 1
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
                try:
                    data = r.json()
                except ValueError:
                    # Thỉnh thoảng body rỗng/cụt dù header là JSON; coi như lỗi tạm thời.
                    log.warning("  body không phải JSON (%d byte) cho %s, thử lại", len(r.content), url)
                    time.sleep(10 * (attempt + 1))
                    continue
                self.streak = 0
                return data
            if r.status_code in (200, 403, 429):
                self.blocks += 1
                self.streak += 1
                if self.streak > self.cfg["max_blocks"]:
                    raise Blocked(f"bị chặn {self.streak} lần liên tiếp, dừng — lần chạy sau sẽ tiếp tục từ chỗ dở")
                log.warning("  bị chặn (HTTP %s, %s) ở request %s thứ %d", r.status_code,
                            r.headers.get("content-type"), kind, self.listing_used if kind == "listing" else self.n)
                if kind == "listing":
                    self.burst = max(5, int(self.burst * 0.75))
                    self._rest(self.cfg["block_wait"], f"giảm đợt listing còn {self.burst} request")
                else:
                    self.interval[kind] = min(self.interval[kind] * 1.5, 60.0)
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
    path.parent.mkdir(parents=True, exist_ok=True)
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


def node_name(cat: int, slug: str | None) -> str:
    return f"{cat}-{slug}" if slug else str(cat)


def cat_file(out: Path, kind: str, node: Path) -> Path:
    return out / kind / node.parent / f"{node.name}.jsonl.gz"


def kind_files(out: Path, kind: str) -> list[Path]:
    return sorted((out / kind).rglob("*.jsonl.gz"))


class State:

    def __init__(self, path: Path):
        self.path = path
        self.d = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        for k in ("detail", "images", "reviews", "sellers"):
            self.d.setdefault(k, {})
        self.dirty = 0

    def __getitem__(self, k: str) -> dict:
        return self.d[k]

    def touch(self) -> None:
        self.dirty += 1
        if self.dirty >= 20:
            self.save()

    def save(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.d), encoding="utf-8")
        tmp.replace(self.path)
        self.dirty = 0


def _lock(path: Path):
    # Hai tiến trình crawl song song dùng chung quota listing và sẽ bị chặn ngay; khoá tự nhả khi tiến trình chết.
    f = open(path, "a+")
    try:
        if os.name == "nt":
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        f.close()
        return None
    return f


def _total(paging: dict) -> int:
    digits = "".join(ch for ch in str(paging.get("total_text", "")) if ch.isdigit())
    return int(digits) if digits else paging.get("total", 0)


def stage_listing(c: Client, out: Path, cfg: dict) -> None:
    done, firsts = set(), {}
    for fp in kind_files(out, "listing"):
        for x in read_lines(fp):
            key = (x["_req"]["category"], x["_req"]["page"])
            done.add(key)
            if key[1] == 1:
                firsts[key[0]] = x["payload"]

    def page(node: Path, cat: int, p: int) -> dict | None:
        res = c.get(f"{API}/products", {"category": cat, "limit": PAGE_SIZE, "page": p}, "listing")
        if res is not None:
            append(cat_file(out, "listing", node), {"category": cat, "page": p}, res)
            done.add((cat, p))
        return res

    def walk(node: Path, cat: int, depth: int) -> None:
        first = firsts.get(cat) or page(node, cat, 1)
        if first is None:
            return
        total = _total(first["paging"])
        if total > PAGE_SIZE * PAGE_CAP:
            kids = c.get(f"{API}/categories", {"parent_id": cat}, "listing")
            children = (kids or {}).get("data") or []
            if children:
                append(out / "categories.jsonl.gz", {"parent_id": cat}, kids)
                log.info("  %s cat %s: %s sp > cap, chia %d danh mục con", "  " * depth, cat, total, len(children))
                for ch in children:
                    if ch["id"] not in cfg["exclude"]:
                        walk(node / node_name(ch["id"], ch.get("url_key")), ch["id"], depth + 1)
                return
            log.warning("  cat %s: %s sp nhưng không có danh mục con, chỉ lấy được %d", cat, total,
                        PAGE_SIZE * PAGE_CAP)
        last = min(first["paging"].get("last_page", 1), PAGE_CAP)
        for p in range(2, last + 1):
            if (cat, p) not in done:
                page(node, cat, p)
        log.info("  %s cat %s: %s sp, %d trang", "  " * depth, cat, total, last)

    for root, slug in cfg["roots"].items():
        walk(Path(node_name(root, slug)), root, 0)


def listing_products(out: Path) -> dict[int, dict]:
    # Trang 1 của danh mục cha trùng sản phẩm với danh mục con: gán sản phẩm vào danh mục sâu nhất.
    prods = {}
    for fp in kind_files(out, "listing"):
        rel = fp.relative_to(out / "listing")
        for x in read_lines(fp):
            for it in x["payload"].get("data") or []:
                old = prods.get(it["id"])
                if old is None or len(rel.parts) >= len(old["_file"].parts):
                    prods[it["id"]] = {**it, "_file": rel}
    return prods


def _stale(last: str | None, days: int, today: date) -> bool:
    return last is None or date.fromisoformat(last) <= today - timedelta(days=days)


def _progress(stage: str, i: int, n: int, t0: float) -> None:
    if i % 200 == 0 or i == n:
        el = time.monotonic() - t0
        eta = el / i * (n - i)
        log.info("  %s %d/%d (%.2f req/s, còn ~%dm)", stage, i, n, i / el, eta / 60)


def stage_detail(c: Client, out: Path, cfg: dict, st: State, today: date, limit: int | None,
                 deadline: float | None = None) -> None:
    prods = listing_products(out)
    todo = [p for pid, p in prods.items() if _stale(st["detail"].get(str(pid)), cfg["detail_refresh_days"], today)]
    todo = todo[:limit] if limit else todo
    log.info("  detail: %d/%d sản phẩm cần lấy", len(todo), len(prods))
    t0 = time.monotonic()
    for i, p in enumerate(todo, 1):
        if deadline and time.monotonic() >= deadline:
            return
        req = {"id": p["id"], "spid": p.get("seller_product_id")}
        res = c.get(f"{API}/products/{p['id']}", {"platform": "web", "spid": req["spid"]})
        if res is not None:
            append(out / "detail" / p["_file"], req, res)
            st["detail"][str(p["id"])] = today.isoformat()
            st.touch()
        _progress("detail", i, len(todo), t0)


def detail_products(out: Path) -> dict[int, dict]:
    """Return the latest detail payload for each product in this snapshot."""
    products = {}
    for fp in kind_files(out, "detail"):
        for row in read_lines(fp):
            product_id = row.get("_req", {}).get("id")
            if product_id is not None:
                products[int(product_id)] = row.get("payload") or {}
    return products


def image_urls(payload: dict) -> list[str]:
    urls = []
    for key in ("thumbnail_url", "image_url"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith("http"):
            urls.append(value)
    for image in payload.get("images") or []:
        if isinstance(image, str):
            candidates = [image]
        elif isinstance(image, dict):
            candidates = [image.get(k) for k in ("large_url", "url", "base_url", "thumbnail_url")]
        else:
            candidates = []
        urls.extend(url for url in candidates if isinstance(url, str) and url.startswith("http"))
    return list(dict.fromkeys(urls))


def image_extension(url: str, content_type: str) -> str:
    if "jpeg" in content_type or "jpg" in content_type:
        return "jpg"
    if "png" in content_type:
        return "png"
    if "webp" in content_type:
        return "webp"
    suffix = url.split("?", 1)[0].rsplit(".", 1)[-1].lower()
    return suffix if suffix in {"jpg", "jpeg", "png", "webp", "gif"} else "jpg"


def stage_images(c: Client, out: Path, cfg: dict, st: State, today: date, limit: int | None) -> None:
    products = detail_products(out)
    todo = [(pid, payload) for pid, payload in products.items()
            if _stale(st["images"].get(str(pid)), cfg["image_refresh_days"], today)]
    todo = todo[:limit] if limit else todo
    log.info("  images: %d/%d products need downloading", len(todo), len(products))
    t0 = time.monotonic()
    for i, (product_id, payload) in enumerate(todo, 1):
        saved = 0
        for index, url in enumerate(image_urls(payload), 1):
            try:
                response = c.s.get(url, timeout=45)
                response.raise_for_status()
            except requests.RequestException as exc:
                log.warning("  image download failed for %s: %s", product_id, exc.__class__.__name__)
                continue
            content_type = response.headers.get("content-type", "").lower()
            if not content_type.startswith("image/"):
                log.warning("  non-image response for product %s", product_id)
                continue
            ext = image_extension(url, content_type)
            target = paths().lake / "images" / "tiki" / today.isoformat() / str(product_id) / f"{index}.{ext}"
            target.write_bytes(response.content, content_type.split(";", 1)[0])
            saved += 1
        if saved:
            st["images"][str(product_id)] = today.isoformat()
            st.touch()
        _progress("images", i, len(todo), t0)


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
            append(out / "reviews" / p["_file"], req, res)
            if pg >= (res.get("paging") or {}).get("last_page", 0):
                break
        if ok:
            st["reviews"][str(p["id"])] = p["review_count"]
            st.touch()
        _progress("reviews", i, len(todo), t0)


def run(stages: list[str] | None = None, limit: int | None = None, roots: list[int] | None = None) -> None:
    cfg = dict(load_config()["crawl"]["tiki"])
    if roots:
        cfg["roots"] = {r: cfg["roots"].get(r) for r in roots}
    root = paths().raw / "tiki"
    root.mkdir(parents=True, exist_ok=True)
    lock = _lock(root / ".lock")
    if lock is None:
        log.warning("[tiki] đang có tiến trình crawl khác chạy, bỏ qua lần này")
        return
    # Ngày theo giờ máy: lịch chạy 02:00 giờ VN vẫn là ngày hôm trước theo UTC.
    today = date.today()
    out = root / today.isoformat()
    out.mkdir(parents=True, exist_ok=True)
    st = State(root / "_state.json")
    c = Client(cfg)
    t0 = time.monotonic()
    try:
        for s in stages or STAGES:
            log.info("[tiki] %s → %s", s, out)
            if s == "listing":
                if "detail" in (stages or STAGES):
                    c.idle = lambda end: stage_detail(c, out, cfg, st, today, limit, end)
                stage_listing(c, out, cfg)
                c.idle = None
            else:
                globals()[f"stage_{s}"](c, out, cfg, st, today, limit)
    finally:
        st.save()
        lock.close()
        log.info("[tiki] %d request trong %.1f phút, bị chặn %d lần", c.n, (time.monotonic() - t0) / 60, c.blocks)


def forever(stages: list[str] | None = None, roots: list[int] | None = None) -> None:
    # Mỗi ngày (giờ máy, như run()) một snapshot listing; bị chặn thì nghỉ rồi chạy lại, tiếp tục từ chỗ dở.
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # để finally của run() kịp lưu _state.json
    cooldown = load_config()["crawl"]["tiki"]["blocked_cooldown"]
    while True:
        day = date.today()
        try:
            run(stages, None, roots)
        except Blocked as e:
            log.warning("[tiki] %s; thử lại sau %d phút", e, cooldown / 60)
            time.sleep(cooldown)
            continue
        # Lượt chạy kéo qua nửa đêm thì vào ngay snapshot ngày mới, không bỏ mất ngày nào.
        nxt = datetime.combine(day + timedelta(days=1), datetime.min.time())
        wait = (nxt - datetime.now()).total_seconds()
        if wait > 0:
            log.info("[tiki] xong snapshot %s, chờ tới %s", day, nxt.strftime("%Y-%m-%d %H:%M"))
            time.sleep(wait + 60)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--stages", nargs="*", choices=STAGES)
    ap.add_argument("--limit", type=int, help="giới hạn số mục mỗi stage (chạy thử)")
    ap.add_argument("--roots", nargs="*", type=int, help="ghi đè danh mục gốc")
    ap.add_argument("--forever", action="store_true", help="chạy mỗi ngày một lần, tự thử lại khi bị chặn")
    a = ap.parse_args()
    if a.forever:
        forever(a.stages, a.roots)
    else:
        run(a.stages, a.limit, a.roots)
