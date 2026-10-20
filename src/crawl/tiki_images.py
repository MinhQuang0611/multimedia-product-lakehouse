from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.common.config import get_logger, load_config, paths
from src.crawl.tiki import listing_products

log = get_logger("crawl.tiki_images")

_local = threading.local()


def _session(ua: str) -> requests.Session:
    if not hasattr(_local, "s"):
        _local.s = requests.Session()
        _local.s.headers.update({"User-Agent": ua, "Referer": "https://tiki.vn/"})
    return _local.s


def main_images(root: Path) -> dict[int, str]:
    # Ảnh chính = thumbnail_url của listing; snapshot mới hơn ghi đè snapshot cũ.
    urls = {}
    for day in sorted(d for d in root.iterdir() if d.is_dir() and d.name[:2] == "20"):
        for pid, p in listing_products(day).items():
            if p.get("thumbnail_url"):
                urls[pid] = p["thumbnail_url"]
    return urls


def fetch(pid: int, url: str, dest: Path, cfg: dict) -> dict | None:
    s = _session(cfg["user_agent"])
    for attempt in range(4):
        try:
            r = s.get(url, timeout=30)
        except requests.RequestException as e:
            log.warning("  ảnh %s: lỗi mạng (%s), thử lại", pid, e.__class__.__name__)
            time.sleep(5 * (attempt + 1))
            continue
        if r.status_code == 404:
            return None
        if r.status_code == 200 and r.headers.get("content-type", "").startswith("image/"):
            tmp = dest.with_suffix(".part")
            tmp.write_bytes(r.content)
            tmp.replace(dest)
            return {"product_id": pid, "url": url, "path": dest.name, "bytes": len(r.content),
                    "sha1": hashlib.sha1(r.content).hexdigest(),
                    "_crawl_ts": datetime.now(timezone.utc).isoformat(timespec="seconds")}
        log.warning("  ảnh %s: HTTP %s (%s), thử lại", pid, r.status_code, r.headers.get("content-type"))
        time.sleep(10 * (attempt + 1))
    return None


def run(limit: int | None = None) -> None:
    cfg = load_config()["crawl"]["tiki"]
    root = paths().raw / "tiki"
    out = root / "images" / "main"
    out.mkdir(parents=True, exist_ok=True)
    manifest = out / "_manifest.jsonl"

    urls = main_images(root)
    todo = [(pid, u) for pid, u in urls.items() if not (out / f"{pid}.jpg").exists()]
    todo = todo[:limit] if limit else todo
    log.info("[tiki] ảnh chính: %d/%d sản phẩm cần tải → %s", len(todo), len(urls), out)

    t0, ok, miss = time.monotonic(), 0, 0
    with ThreadPoolExecutor(cfg["image_workers"]) as ex, open(manifest, "a", encoding="utf-8") as mf:
        futs = [ex.submit(fetch, pid, u, out / f"{pid}.jpg", cfg) for pid, u in todo]
        for i, f in enumerate(as_completed(futs), 1):
            rec = f.result()
            if rec:
                mf.write(json.dumps(rec) + "\n")
                ok += 1
            else:
                miss += 1
            if i % 500 == 0 or i == len(todo):
                el = time.monotonic() - t0
                log.info("  ảnh %d/%d (%.1f ảnh/s, còn ~%dm)", i, len(todo), i / el, el / i * (len(todo) - i) / 60)
                mf.flush()
    log.info("[tiki] ảnh chính: tải %d, lỗi %d trong %.1f phút", ok, miss, (time.monotonic() - t0) / 60)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, help="chỉ tải N ảnh (chạy thử)")
    run(ap.parse_args().limit)
