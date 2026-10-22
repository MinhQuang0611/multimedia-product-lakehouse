from __future__ import annotations

import json
import random
import time
from datetime import date
from pathlib import Path

import requests

from src.common.config import get_logger, paths
from src.crawl.tiki import _lock, read_lines

log = get_logger("crawl.http")


class Http:

    def __init__(self, min_interval: float, user_agent: str, headers: dict | None = None):
        self.s = requests.Session()
        self.s.headers.update({"User-Agent": user_agent, "Accept": "application/json", **(headers or {})})
        self.min_interval = min_interval
        self.last = 0.0
        self.n = 0

    def get_json(self, url: str, params: dict | None = None):
        for attempt in range(4):
            gap = self.min_interval * random.uniform(1.0, 1.3) - (time.monotonic() - self.last)
            if gap > 0:
                time.sleep(gap)
            self.last = time.monotonic()
            self.n += 1
            try:
                r = self.s.get(url, params=params, timeout=30)
            except requests.RequestException as e:
                log.warning("  lỗi mạng (%s) cho %s, thử lại", e.__class__.__name__, url)
                time.sleep(10 * (attempt + 1))
                continue
            if r.status_code == 404:
                return None
            if r.status_code == 200 and "json" in r.headers.get("content-type", ""):
                try:
                    return r.json()
                except ValueError:
                    pass
            log.warning("  HTTP %s (%s) cho %s, thử lại", r.status_code, r.headers.get("content-type"), url)
            time.sleep(30 * (attempt + 1))
        log.error("  bỏ qua %s %s sau 4 lần thử", url, params)
        return None


def day_dir(source: str) -> tuple[Path | None, object]:
    # Mỗi nguồn một khoá riêng: các nguồn khác host nên chạy song song được với nhau.
    root = paths().raw / source
    root.mkdir(parents=True, exist_ok=True)
    lock = _lock(root / ".lock")
    if lock is None:
        log.warning("[%s] đang có tiến trình crawl khác chạy, bỏ qua lần này", source)
        return None, None
    out = root / date.today().isoformat()
    out.mkdir(parents=True, exist_ok=True)
    return out, lock


def req_key(req: dict) -> str:
    return json.dumps(req, sort_keys=True)


def done_reqs(files) -> set[str]:
    return {req_key(x["_req"]) for f in files for x in read_lines(f)}
