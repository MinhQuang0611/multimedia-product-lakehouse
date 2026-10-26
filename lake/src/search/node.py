import asyncio
import base64
import binascii
import os
import socket
import time
from contextlib import asynccontextmanager
from typing import Literal

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from src.search.shard import Shard

# Một node = một tiến trình giữ Shard của vài nguồn (cửa hàng). Không cần GPU/torch: coordinator gửi vector đã tính.
# Cấu hình qua biến môi trường để cùng một image chạy được mọi node trong docker-compose.search.yml.
NODE_ID = os.environ.get("NODE_ID", socket.gethostname())
SOURCES = [s for s in os.environ.get("NODE_SOURCES", "").split(",") if s] or None
DELAY_MS = float(os.environ.get("NODE_DELAY_MS", "0"))  # giả lập node chậm (thí nghiệm timeout)
MAX_K = 100
STATE: dict = {}


def encode_vectors(Q: np.ndarray) -> str:
    return base64.b64encode(np.ascontiguousarray(Q, dtype=np.float32).tobytes()).decode()


def decode_vectors(b64: str, dim: int) -> np.ndarray:
    try:
        raw = base64.b64decode(b64, validate=True)
    except (binascii.Error, ValueError) as e:
        raise ValueError("vectors không phải base64") from e
    if not raw or len(raw) % (4 * dim):
        raise ValueError(f"độ dài vectors ({len(raw)} byte) không khớp dim={dim}")
    return np.frombuffer(raw, dtype=np.float32).reshape(-1, dim)


class Filters(BaseModel):
    """Bộ lọc dùng chung cho coordinator và node: sai kiểu bị từ chối (400/422) thay vì làm node lỗi 500."""

    model_config = ConfigDict(extra="forbid")
    source: list[str] | None = None
    category_l1: list[str] | None = None
    category_l2: list[str] | None = None
    audience: list[str] | None = None
    price_min: float | None = Field(None, ge=0)
    price_max: float | None = Field(None, ge=0)
    in_stock: bool | None = None


class SearchRequest(BaseModel):
    kind: Literal["image", "text"]
    vectors: str  # float32 base64: giữ nguyên từng bit, gọn hơn JSON số thực
    dim: int = Field(ge=1)
    qregions: list[str]
    k: int = Field(10, ge=1, le=MAX_K)
    filters: Filters | None = None
    exclude_sha: str | None = None
    version: str  # model_version của vector truy vấn; khác chỉ mục của node thì từ chối


@asynccontextmanager
async def lifespan(app: FastAPI):
    STATE["shard"] = Shard(sources=SOURCES, text=os.environ.get("NODE_TEXT", "1") == "1")
    STATE["started"] = time.time()
    STATE["served"] = 0
    yield


app = FastAPI(title="search-node", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    shard: Shard = STATE["shard"]
    return {"node": NODE_ID, "status": "ok", "uptime_s": round(time.time() - STATE["started"], 1),
            "served": STATE["served"], **shard.info()}


def main(argv: list[str] | None = None) -> int:
    import argparse

    import uvicorn

    global NODE_ID, SOURCES, DELAY_MS
    ap = argparse.ArgumentParser(prog="run.py search node", description="Một node tìm kiếm (không cần Docker)")
    ap.add_argument("--id", default=NODE_ID)
    ap.add_argument("--sources", nargs="*", help="mặc định: mọi nguồn")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=18711)
    ap.add_argument("--delay-ms", type=float, default=DELAY_MS)
    a = ap.parse_args(argv)
    NODE_ID, SOURCES, DELAY_MS = a.id, a.sources or None, a.delay_ms
    uvicorn.run(app, host=a.host, port=a.port, log_level="warning")
    return 0


@app.post("/search")
async def search(req: SearchRequest) -> dict:
    shard: Shard = STATE["shard"]
    if req.kind not in shard.index:
        raise HTTPException(400, f"node {NODE_ID} không có chỉ mục {req.kind}")
    mine = shard.index[req.kind].version
    if req.version != mine:
        raise HTTPException(409, f"model_version khác: truy vấn {req.version}, node {mine}")
    try:
        Q = decode_vectors(req.vectors, req.dim)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
    if Q.shape[1] != shard.index[req.kind].index.d or len(req.qregions) != len(Q):
        raise HTTPException(400, f"cần {shard.index[req.kind].index.d} chiều và đủ qregions cho {len(Q)} vector")
    if DELAY_MS:
        await asyncio.sleep(DELAY_MS / 1000)
    t0 = time.perf_counter()
    filters = req.filters.model_dump(exclude_none=True) if req.filters else None
    hits = shard.rank(req.kind, Q, req.qregions, req.k, filters, req.exclude_sha)
    STATE["served"] += 1
    return {"node": NODE_ID, "results": hits, "search_ms": round((time.perf_counter() - t0) * 1000, 3)}
