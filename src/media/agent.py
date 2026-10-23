import hashlib
import json
import mimetypes
import shutil
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from urllib.parse import urlparse

import duckdb
import requests

from src.common.core import LAKE, S, Agent, Context, Task, layer, log, now_iso

try:
    from PIL import Image
except ImportError:
    Image = None

MAX_ATTEMPTS = 5
EXT = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif", "image/avif": "avif",
       "video/mp4": "mp4", "video/webm": "webm", "video/quicktime": "mov"}


def manifest_path(source: str):
    return layer("media", "manifest", f"{source}.jsonl")


def load_manifest(source: str) -> dict[str, dict]:
    out = {}
    p = manifest_path(source)
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            try:
                r = json.loads(line)
                out[r["media_key"]] = r
            except (ValueError, KeyError):
                continue
    return out


class Budget:
    def __init__(self):
        self.files, self.bytes = 0, 0
        self.lock = threading.Lock()
        self.stop_reason: str | None = None

    def ok(self) -> bool:
        if self.stop_reason:
            return False
        free_gb = shutil.disk_usage(LAKE.parent).free / 1024**3
        with self.lock:
            if self.files >= S.media_max_files_per_run:
                self.stop_reason = f"đủ {S.media_max_files_per_run:,} file"
            elif self.bytes >= S.media_max_bytes_per_run:
                self.stop_reason = f"đủ {S.media_max_bytes_per_run / 1024**3:.1f} GB"
            elif free_gb < S.media_min_free_gb:
                self.stop_reason = f"đĩa còn {free_gb:.0f} GB < {S.media_min_free_gb} GB"
        return self.stop_reason is None

    def add(self, n: int) -> None:
        with self.lock:
            self.files += 1
            self.bytes += n


def pending(source: str, done: dict[str, dict]) -> list[dict]:
    """Media chuẩn hoá chưa có file: ưu tiên ảnh chính, vị trí thấp, snapshot mới; mỗi sản phẩm tối đa N cái."""
    glob = (layer("normalized", "media", f"source={source}") / "*" / "*.parquet").as_posix()
    if not list(layer("normalized", "media", f"source={source}").glob("*/*.parquet")):
        return []
    con = duckdb.connect()
    rows = con.execute(f"""
        WITH m AS (
            SELECT media_key, any_value(product_key) AS product_key, any_value(media_type) AS media_type,
                   any_value(url) AS url, any_value(source_path) AS source_path, min("position") AS pos
            FROM read_parquet('{glob}') GROUP BY media_key)
        SELECT media_key, product_key, media_type, url, source_path, pos FROM m
        QUALIFY row_number() OVER (PARTITION BY product_key ORDER BY pos, media_key) <= {S.media_max_per_product}
        ORDER BY pos, media_key""").fetchall()
    con.close()
    cols = ["media_key", "product_key", "media_type", "url", "source_path", "position"]
    out = []
    for r in map(lambda r: dict(zip(cols, r)), rows):
        prev = done.get(r["media_key"])
        if prev and (prev["status"] == "ok" or prev.get("attempts", 0) >= MAX_ATTEMPTS):
            continue
        if r["media_type"] == "video" and not S.media_video:
            continue
        r["attempts"] = prev.get("attempts", 0) if prev else 0
        out.append(r)
    # Mục chưa thử lần nào đi trước, mục đã lỗi thử lại sau (sort ổn định giữ thứ tự ưu tiên ảnh chính).
    return sorted(out, key=lambda r: r["attempts"] > 0)


class MediaAgent(Agent):
    """Tải ảnh/video theo URL (hoặc đăng ký file cục bộ có sẵn) vào kho nội dung-địa-chỉ + manifest."""

    name = "media"

    def run(self, task: Task, ctx: Context) -> dict:
        source = task.params["source"]
        done = load_manifest(source)
        todo = pending(source, done)
        stats = defaultdict(int)
        if not todo:
            return dict(stats)
        self.say(ctx, task, f"{source}: {len(todo):,} media cần xử lý")
        budget, mlock = Budget(), threading.Lock()
        hosts: dict[str, threading.Semaphore] = defaultdict(lambda: threading.Semaphore(S.media_host_concurrency))
        local = threading.local()
        out = manifest_path(source)
        out.parent.mkdir(parents=True, exist_ok=True)

        def record(rec: dict) -> None:
            with mlock, open(out, "a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            stats[rec["status"]] += 1

        def work(item: dict) -> None:
            if not budget.ok():
                stats["deferred"] += 1
                return
            base = {"media_key": item["media_key"], "source": source, "product_key": item["product_key"],
                    "url": item["url"], "attempts": item["attempts"] + 1, "fetched_at": now_iso()}
            try:
                if item["source_path"]:
                    rec = {**base, **self.register(item["source_path"])}
                else:
                    sess = getattr(local, "s", None)
                    if sess is None:
                        sess = local.s = requests.Session()
                    host = urlparse(item["url"]).netloc
                    with hosts[host]:
                        rec = {**base, **self.fetch(sess, item["url"], source, item["media_type"])}
                    budget.add(rec.get("bytes") or 0)
                rec["status"] = "ok"
            except Exception as e:
                rec = {**base, "status": "failed", "error": f"{e.__class__.__name__}: {str(e)[:200]}"}
            record(rec)

        t0 = time.monotonic()
        with ThreadPoolExecutor(S.media_workers) as ex:
            list(ex.map(work, todo))
        stats["seconds"] = round(time.monotonic() - t0, 1)
        stats["downloaded_mb"] = round(budget.bytes / 1024**2, 1)
        if budget.stop_reason:
            stats["stopped"] = budget.stop_reason
            log.warning("[%s] dừng tải media: %s", task.id, budget.stop_reason)
        return dict(stats)

    @staticmethod
    def dims(data: bytes) -> tuple[int | None, int | None]:
        if Image is None:
            return None, None
        try:
            with Image.open(BytesIO(data)) as im:
                im.verify()
            with Image.open(BytesIO(data)) as im:
                return im.size
        except Exception as e:
            raise ValueError(f"ảnh hỏng ({e.__class__.__name__})")

    def register(self, path: str) -> dict:
        # File cục bộ có sẵn (H&M, Shopee): không copy, chỉ băm và ghi kích thước.
        data = open(path, "rb").read()
        w, h = self.dims(data)
        return {"local_path": path, "external": True, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
                "width": w, "height": h, "content_type": mimetypes.guess_type(path)[0]}

    @staticmethod
    def open(sess: requests.Session, url: str):
        # Lỗi DNS/SSL/429/5xx thường chỉ là nhất thời (lúc crawl song song nhiều nguồn): thử lại ngay, không tốn lượt.
        for i in range(3):
            try:
                r = sess.get(url, timeout=45, stream=True, headers={"User-Agent": S.crawl_tiki_user_agent})
            except requests.RequestException:
                if i == 2:
                    raise
                time.sleep(3 * 2**i)
                continue
            if r.status_code in (429, 500, 502, 503, 504) and i < 2:
                r.close()
                time.sleep(3 * 2**i)
                continue
            return r

    def fetch(self, sess: requests.Session, url: str, source: str, kind: str) -> dict:
        limit = S.media_video_max_mb * 1024**2 if kind == "video" else 40 * 1024**2
        with self.open(sess, url) as r:
            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}")
            ctype = r.headers.get("content-type", "").split(";")[0].strip().lower()
            if not ctype.startswith(("image/", "video/")):
                raise ValueError(f"content-type {ctype or '?'}")
            buf = BytesIO()
            for chunk in r.iter_content(256 * 1024):
                buf.write(chunk)
                if buf.tell() > limit:
                    raise ValueError("quá lớn")
        data = buf.getvalue()
        if not data:
            raise ValueError("rỗng")
        w = h = None
        if ctype.startswith("image/"):
            w, h = self.dims(data)
        sha = hashlib.sha256(data).hexdigest()
        ext = EXT.get(ctype) or url.split("?", 1)[0].rsplit(".", 1)[-1].lower()[:4] or "bin"
        rel = f"media/files/{source}/{sha[:2]}/{sha}.{ext}"
        dest = LAKE / rel
        if not dest.exists():
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(".part")
            tmp.write_bytes(data)
            tmp.replace(dest)
        return {"local_path": rel, "external": False, "sha256": sha, "bytes": len(data), "width": w, "height": h,
                "content_type": ctype}
