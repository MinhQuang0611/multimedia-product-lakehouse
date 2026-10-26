import argparse
import asyncio
import hashlib
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from io import BytesIO
from typing import Literal

import httpx
import numpy as np
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, Field, ValidationError

from lib.config import get_logger
from src.search.catalog import load_rgb
from src.search.engine import REGIONS, QueryEncoder
from src.search.node import MAX_K, Filters, decode_vectors, encode_vectors
from src.search.shard import top_k
from src.search.topology import TOPOLOGIES, node_urls

log = get_logger("search.coordinator")
MAX_UPLOAD = 15 * 1024**2
MAX_PIXELS = 40_000_000  # ảnh lớn hơn bị từ chối trước khi giải mã (giải mã 178 MP tốn ~0,5 GB RAM)


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


def decode_image(data: bytes) -> Image.Image:
    """Ảnh tải lên → RGB; lỗi định dạng/kích thước thành ValueError (trả 400), không phải 500."""
    if len(data) > MAX_UPLOAD:
        raise ValueError(f"ảnh quá lớn (> {MAX_UPLOAD // 1024**2} MB)")
    try:
        with Image.open(BytesIO(data)) as im:
            if im.width * im.height > MAX_PIXELS:
                raise ValueError(f"ảnh quá nhiều điểm ảnh ({im.width}×{im.height})")
        return load_rgb(BytesIO(data))
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError) as e:
        raise ValueError(f"không đọc được ảnh ({e.__class__.__name__})") from e


@dataclass
class NodeState:
    name: str
    url: str
    healthy: bool = False
    failures: int = 0  # số lần gọi lỗi liên tiếp
    # Cầu dao: closed (gọi bình thường) → open (bỏ qua node) → half_open (health check đã qua sau open_seconds,
    # cho đúng một truy vấn thử) → trial (truy vấn thử đang chạy) → closed nếu thử thành công, open nếu thất bại.
    state: str = "closed"
    opened_at: float = 0.0
    info: dict = field(default_factory=dict)
    served: int = 0
    errors: int = 0

    @property
    def breaker_open(self) -> bool:
        return self.state != "closed"


class Coordinator:
    """Nhận truy vấn, tính vector một lần, gửi song song tới mọi node, gộp top-k của từng node thành top-k toàn cục.

    Mỗi sản phẩm nằm ở đúng một node và mỗi node trả top-k chính xác của mình, nên top-k toàn cục luôn nằm trong hợp
    các top-k cục bộ: gộp rồi xếp lại cho kết quả y hệt tìm trên một chỉ mục chung (vector đã L2-normalize, cosine so
    được giữa các node, không cần chuẩn hoá lại điểm)."""

    def __init__(self, nodes: dict[str, str], timeout: float = 2.0, retries: int = 1, cache_size: int = 512,
                 fail_threshold: int = 3, open_seconds: float = 10.0, health_every: float = 5.0, crops: bool = True):
        self.nodes = {n: NodeState(n, u) for n, u in nodes.items()}
        self.timeout, self.retries, self.cache_size = timeout, retries, cache_size
        self.fail_threshold, self.open_seconds, self.health_every = fail_threshold, open_seconds, health_every
        self.query = QueryEncoder(crops=crops)
        self.lock = threading.Lock()  # một model trên GPU, gọi tuần tự từ luồng phụ
        self.cache: OrderedDict[str, dict] = OrderedDict()
        self.stats = {"requests": 0, "cache_hits": 0, "partial": 0}
        self.client: httpx.AsyncClient | None = None
        self.health_task: asyncio.Task | None = None

    async def start(self) -> None:
        self.client = httpx.AsyncClient(limits=httpx.Limits(max_connections=200, max_keepalive_connections=50))
        await asyncio.to_thread(self.query.warmup)
        await self.check_all()
        self.health_task = asyncio.create_task(self.health_loop())

    async def stop(self) -> None:
        if self.health_task:
            self.health_task.cancel()
        if self.client:
            await self.client.aclose()

    async def health_loop(self) -> None:
        while True:
            await asyncio.sleep(self.health_every)
            await self.check_all()

    async def check_all(self) -> None:
        await asyncio.gather(*(self.check(n) for n in self.nodes.values()))

    async def check(self, node: NodeState) -> None:
        try:
            r = await self.client.get(f"{node.url}/health", timeout=1.0)
            r.raise_for_status()
            node.info = r.json()
            if not node.healthy:
                log.info("node %s sẵn sàng: %s", node.name, node.info.get("sources") or "tất cả nguồn")
            node.healthy = True
            # /health trả lời được chưa chắc /search nhanh (node quá tải vẫn trả health): không đóng cầu dao ngay mà
            # chuyển nửa mở, để một truy vấn thật quyết định đóng (thành công) hay mở lại (thất bại).
            if node.state == "open" and time.monotonic() - node.opened_at >= self.open_seconds:
                node.state = "half_open"
        except (httpx.HTTPError, ValueError):
            if node.healthy:
                log.warning("node %s không phản hồi health check", node.name)
            node.healthy = False

    def trip(self, node: NodeState, why: str) -> None:
        node.state, node.opened_at, node.healthy = "open", time.monotonic(), False
        log.warning("mở cầu dao node %s (%s)", node.name, why)

    async def call(self, node: NodeState, payload: dict) -> dict:
        t0 = time.perf_counter()
        want = node.info.get("models", {}).get(payload["kind"], {}).get("version")
        if want and want != payload["version"]:
            return {"status": "version_mismatch", "ms": 0.0, "attempts": 0}
        if node.state in ("open", "trial"):
            return {"status": "circuit_open", "ms": 0.0, "attempts": 0}
        trial = node.state == "half_open"
        if trial:
            node.state = "trial"  # truy vấn đồng thời khác vẫn bỏ qua node cho tới khi thử xong
        extra = {"trial": True} if trial else {}
        status, attempt = "error", 0
        # Chỉ thử lại lỗi kết nối (node vừa khởi động lại, mạng chập chờn). Hết thời gian chờ thì không thử lại: node
        # đang chậm thì lần sau cũng chậm, và thử lại làm thời gian chờ xấu nhất tăng gấp đôi.
        while attempt <= self.retries:
            attempt += 1
            try:
                r = await self.client.post(f"{node.url}/search", json=payload, timeout=self.timeout)
                if 400 <= r.status_code < 500:
                    # Lỗi phía truy vấn, không phải node hỏng: không tính vào cầu dao, trả lại lượt thử.
                    if trial:
                        node.state = "half_open"
                    st = "version_mismatch" if r.status_code == 409 else "rejected"
                    return {"status": st, "ms": ms(t0), "attempts": attempt, "detail": r.text[:200]}
                r.raise_for_status()
                body = r.json()
                results, node_ms = body["results"], body["search_ms"]
                node.failures, node.state = 0, "closed"
                node.served += 1
                return {"status": "ok", "ms": ms(t0), "attempts": attempt, "node_ms": node_ms, "results": results, **extra}
            except httpx.TimeoutException:
                status = "timeout"
                break
            except httpx.TransportError:
                status = "error"
            except httpx.HTTPStatusError:
                status = "error"
                break
            except (ValueError, KeyError):
                status = "bad_response"
                break
        node.failures += 1
        node.errors += 1
        if trial:
            self.trip(node, f"truy vấn thử thất bại: {status}")
        elif node.failures >= self.fail_threshold:
            self.trip(node, f"{node.failures} lần lỗi liên tiếp")
        return {"status": status, "ms": ms(t0), "attempts": attempt, **extra}

    async def fan_out(self, kind: str, Q: np.ndarray, qregions: list[str], k: int, filters: dict | None,
                      exclude_sha: str | None, version: str) -> dict:
        payload = {"kind": kind, "vectors": encode_vectors(Q), "dim": int(Q.shape[1]), "qregions": qregions, "k": k,
                   "filters": filters, "exclude_sha": exclude_sha, "version": version}
        t0 = time.perf_counter()
        reps = await asyncio.gather(*(self.call(n, payload) for n in self.nodes.values()))
        fan = ms(t0)
        t1 = time.perf_counter()
        hits = [{**h, "node": n} for n, rep in zip(self.nodes, reps) for h in rep.get("results", [])]
        merged = top_k(hits, k)
        report = {n: {key: v for key, v in rep.items() if key != "results"} | {"returned": len(rep.get("results", []))}
                  for n, rep in zip(self.nodes, reps)}
        partial = any(r["status"] != "ok" for r in reps)
        self.stats["partial"] += partial
        return {"results": merged, "partial": partial, "nodes": report, "timing": {"fanout_ms": fan, "merge_ms": ms(t1)}}

    def cached(self, key: str) -> dict | None:
        if key in self.cache:
            self.cache.move_to_end(key)
            self.stats["cache_hits"] += 1
            return self.cache[key]
        return None

    def remember(self, key: str, res: dict) -> None:
        # Không lưu kết quả thiếu node: node sống lại thì truy vấn sau phải thấy đủ.
        if res["partial"] or not self.cache_size:
            return
        self.cache[key] = res
        if len(self.cache) > self.cache_size:
            self.cache.popitem(last=False)

    async def search_image(self, data: bytes, k: int = 10, region: str | None = None, filters: dict | None = None,
                           exclude_sha: str | None = None) -> dict:
        t0 = time.perf_counter()
        self.stats["requests"] += 1
        sha = hashlib.sha256(data).hexdigest()
        key = json.dumps(["image", sha, region, k, filters, exclude_sha], sort_keys=True, default=str)
        if hit := self.cached(key):
            return {**hit, "cache": "hit", "query_sha": sha, "timing": {"total_ms": ms(t0)}}
        im = await asyncio.to_thread(decode_image, data)  # giải mã ngoài khoá GPU: ảnh lớn không chặn truy vấn khác

        def encode():
            with self.lock:
                return self.query.image(im, region, sha)

        q = await asyncio.to_thread(encode)
        res = await self.fan_out("image", q["Q"], q["qregions"], k, filters, exclude_sha, q["version"])
        res = {**res, "regions": q["regions"], "query_sha": sha, "timing": {**q["timing"], **res["timing"]}}
        self.remember(key, res)
        return {**res, "cache": "miss", "timing": {**res["timing"], "total_ms": ms(t0)}}

    async def search_text(self, text: str, k: int = 10, filters: dict | None = None) -> dict:
        t0 = time.perf_counter()
        self.stats["requests"] += 1
        key = json.dumps(["text", text, k, filters], sort_keys=True, default=str)
        if hit := self.cached(key):
            return {**hit, "cache": "hit", "timing": {"total_ms": ms(t0)}}

        def encode():
            with self.lock:
                return self.query.text(text)

        q = await asyncio.to_thread(encode)
        res = await self.fan_out("text", q["Q"], q["qregions"], k, filters, None, q["version"])
        res = {**res, "regions": {}, "timing": {**q["timing"], **res["timing"]}}
        self.remember(key, res)
        return {**res, "cache": "miss", "timing": {**res["timing"], "total_ms": ms(t0)}}

    def health(self) -> dict:
        return {"status": "ok", "nodes": {
            n.name: {"url": n.url, "healthy": n.healthy, "breaker": n.state, "breaker_open": n.breaker_open,
                     "failures": n.failures, "sources": n.info.get("sources"), "products": n.info.get("products"),
                     "vectors": n.info.get("vectors")} for n in self.nodes.values()}}


def filters_dict(f: Filters | None) -> dict | None:
    d = f.model_dump(exclude_none=True) if f else {}
    return d or None


class TextQuery(BaseModel):
    text: str = Field(min_length=1, max_length=500)
    k: int = Field(10, ge=1, le=MAX_K)
    filters: Filters | None = None


class VectorQuery(BaseModel):
    kind: Literal["image", "text"]
    vectors: str
    dim: int = Field(ge=1)
    qregions: list[str]
    k: int = Field(10, ge=1, le=MAX_K)
    filters: Filters | None = None
    exclude_sha: str | None = None
    version: str


def create_app(coord: Coordinator) -> FastAPI:
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await coord.start()
        yield
        await coord.stop()

    app = FastAPI(title="search-coordinator", lifespan=lifespan)

    # Các endpoint đọc/ghi trạng thái (cache, cầu dao) đều là async: chạy trên event loop, không chạy song song
    # trong threadpool với truy vấn đang sửa cùng trạng thái đó.
    @app.get("/health")
    async def health() -> dict:
        return coord.health()

    @app.get("/stats")
    async def stats() -> dict:
        return {**coord.stats, "cache_size": len(coord.cache),
                "nodes": {n.name: {"served": n.served, "errors": n.errors} for n in coord.nodes.values()}}

    @app.post("/cache/clear")
    async def clear() -> dict:
        coord.cache.clear()
        return {"cleared": True}

    @app.post("/search/image")
    async def search_image(file: UploadFile = File(...), k: int = Form(10, ge=1, le=MAX_K),
                           region: str | None = Form(None), filters: str | None = Form(None),
                           exclude_sha: str | None = Form(None)) -> dict:
        if region and region not in REGIONS:
            raise HTTPException(400, f"region phải thuộc {REGIONS}")
        try:
            f = Filters.model_validate_json(filters) if filters else None
        except ValidationError as e:
            raise HTTPException(400, f"filters không hợp lệ: {e.errors(include_url=False)}") from e
        data = await file.read(MAX_UPLOAD + 1)
        try:
            return await coord.search_image(data, k, region or None, filters_dict(f), exclude_sha)
        except ValueError as e:
            raise HTTPException(413 if len(data) > MAX_UPLOAD else 400, str(e)) from e

    @app.post("/search/text")
    async def search_text(q: TextQuery) -> dict:
        return await coord.search_text(q.text, q.k, filters_dict(q.filters))

    @app.post("/search/vectors")
    async def search_vectors(q: VectorQuery) -> dict:
        # Vector tính sẵn (thí nghiệm so tập trung/phân tán dùng đúng một bộ vector cho hai phía); không cache.
        try:
            Q = decode_vectors(q.vectors, q.dim)
        except ValueError as e:
            raise HTTPException(400, str(e)) from e
        return await coord.fan_out(q.kind, Q, q.qregions, q.k, filters_dict(q.filters), q.exclude_sha, q.version)

    return app


def main(argv: list[str] | None = None) -> int:
    import uvicorn

    ap = argparse.ArgumentParser(prog="run.py search coordinator", description="Coordinator tìm kiếm phân tán (cần GPU)")
    ap.add_argument("--topology", choices=list(TOPOLOGIES), default="n3")
    ap.add_argument("--nodes", nargs="*", help="ghi đè: tên=url, ví dụ node-a=http://10.0.0.5:8000")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18700)
    ap.add_argument("--timeout", type=float, default=2.0, help="giây chờ mỗi lần gọi node")
    ap.add_argument("--retries", type=int, default=1, help="số lần thử lại khi lỗi kết nối (không thử lại khi timeout)")
    ap.add_argument("--cache-size", type=int, default=512, help="0 = tắt cache")
    ap.add_argument("--no-crops", action="store_true")
    a = ap.parse_args(argv)
    nodes = dict(x.split("=", 1) for x in a.nodes) if a.nodes else node_urls(a.topology)
    coord = Coordinator(nodes, a.timeout, a.retries, a.cache_size, crops=not a.no_crops)
    log.info("coordinator %s → %s", a.topology, nodes)
    uvicorn.run(create_app(coord), host=a.host, port=a.port, log_level="warning")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
