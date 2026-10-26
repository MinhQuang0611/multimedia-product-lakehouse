import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import numpy as np

from lib.config import ROOT, get_logger
from src.search.catalog import REPORT_DIR, load_rgb
from src.search.engine import QueryEncoder
from src.search.node import encode_vectors
from src.search.shard import Shard
from src.search.topology import TOPOLOGIES

log = get_logger("search.bench")
COMPOSE = ["docker", "compose", "-f", str(ROOT.parent / "docker-compose.search.yml")]
PORT = 18701


def compose(*args: str, env: dict | None = None) -> None:
    subprocess.run([*COMPOSE, *args], check=True, capture_output=True, env={**os.environ, **(env or {})})


def wait_health(url: str, n_nodes: int, timeout: float = 300) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            nodes = httpx.get(f"{url}/health", timeout=2).json()["nodes"]
            if sum(v["healthy"] for v in nodes.values()) == n_nodes:
                return
        except (httpx.HTTPError, ValueError, KeyError):
            pass
        time.sleep(2)
    raise TimeoutError(f"{url}: chưa đủ {n_nodes} node sau {timeout} s")


class Cluster:
    """Bật node của một topology (docker compose) và một coordinator riêng cho thí nghiệm (cache tắt)."""

    def __init__(self, topology: str, timeout: float = 2.0, env: dict | None = None):
        self.topology, self.timeout, self.env = topology, timeout, env
        self.url = f"http://127.0.0.1:{PORT}"

    def __enter__(self):
        compose("--profile", self.topology, "up", "-d", "--wait", env=self.env)
        log.info("topology %s: node đã sẵn sàng", self.topology)
        self.proc = subprocess.Popen(
            [sys.executable, "-W", "ignore", "run.py", "search", "coordinator", "--topology", self.topology, "--port", str(PORT),
             "--cache-size", "0", "--timeout", str(self.timeout)],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_health(self.url, len(TOPOLOGIES[self.topology]))
        return self

    def __exit__(self, *exc):
        self.proc.terminate()
        self.proc.wait(30)
        compose("--profile", self.topology, "down")


def build_queries(enc: QueryEncoder, shard: Shard, n_text: int) -> list[dict]:
    """Truy vấn khác góc (mọi ảnh của sản phẩm ≥2 ảnh, bỏ chính ảnh đó) + tên sản phẩm làm truy vấn văn bản."""
    shas = {}
    for r in shard.media:
        shas.setdefault(r["product_key"], set()).add(r["sha256"])
    out = []
    for r in (r for r in shard.media if len(shas[r["product_key"]]) >= 2):
        q = enc.image(load_rgb(r["path"]), None, r["sha256"])
        out.append({"kind": "image", "Q": q["Q"], "qregions": q["qregions"], "exclude_sha": r["sha256"],
                    "version": q["version"], "target": r["product_key"]})
    rng = np.random.default_rng(7)
    for p in rng.choice(shard.products, n_text, replace=False):
        q = enc.text(p["name"])
        out.append({"kind": "text", "Q": q["Q"], "qregions": ["text"], "exclude_sha": None, "version": q["version"],
                    "target": p["product_key"]})
    log.info("đã tính vector cho %d truy vấn", len(out))
    return out


def payload(q: dict, k: int) -> dict:
    return {"kind": q["kind"], "vectors": encode_vectors(q["Q"]), "dim": int(q["Q"].shape[1]), "qregions": q["qregions"],
            "k": k, "filters": None, "exclude_sha": q["exclude_sha"], "version": q["version"]}


async def run_all(url: str, qs: list[dict], k: int, concurrency: int = 8) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient(timeout=60) as c:
        async def one(q):
            async with sem:
                t0 = time.perf_counter()
                r = (await c.post(f"{url}/search/vectors", json=payload(q, k))).json()
                r["client_ms"] = (time.perf_counter() - t0) * 1000
                return r
        return await asyncio.gather(*(one(q) for q in qs))


def recall(qs: list[dict], res: list[dict], kind: str = "image") -> dict:
    ranks = []
    for q, r in zip(qs, res):
        if q["kind"] != kind:
            continue
        keys = [h["product_key"] for h in r["results"]]
        ranks.append(keys.index(q["target"]) + 1 if q["target"] in keys else 99)
    ranks = np.array(ranks)
    return {"R@1": float((ranks == 1).mean()), "R@10": float((ranks <= 10).mean()), "n": len(ranks)}


def exactness(qs: list[dict], central: list[list[dict]], dist: list[dict]) -> dict:
    same_list = same_scores = 0
    max_diff = 0.0
    for c, d in zip(central, dist):
        ck, dk = [h["product_key"] for h in c], [h["product_key"] for h in d["results"]]
        same_list += ck == dk
        if ck == dk:
            diff = max((abs(a["score"] - b["score"]) for a, b in zip(c, d["results"])), default=0.0)
            max_diff = max(max_diff, diff)
            same_scores += diff == 0.0
    return {"queries": len(qs), "same_top_k": same_list, "bitwise_same_scores": same_scores, "max_abs_score_diff": max_diff}


def pct(xs: list[float]) -> dict:
    a = np.array(xs)
    return {"p50": float(np.percentile(a, 50)), "p95": float(np.percentile(a, 95)), "p99": float(np.percentile(a, 99)),
            "mean": float(a.mean())}


async def latency(url: str, qs: list[dict], k: int, n: int, concurrency: int) -> dict:
    pick = [qs[i % len(qs)] for i in range(n)]
    t0 = time.perf_counter()
    res = await run_all(url, pick, k, concurrency)
    wall = time.perf_counter() - t0
    return {"concurrency": concurrency, "n": n, "qps": n / wall, "client_ms": pct([r["client_ms"] for r in res]),
            "fanout_ms": pct([r["timing"]["fanout_ms"] for r in res]),
            "node_ms": pct([v.get("node_ms", 0) for r in res for v in r["nodes"].values() if v["status"] == "ok"])}


async def sequential(url: str, qs: list[dict], k: int, n: int, gap: float) -> list[dict]:
    # Truy vấn cách nhau `gap` giây để thấy cầu dao qua các lần health check (5 s): mở → nửa mở (thử 1 truy vấn) → mở lại.
    out, start = [], time.perf_counter()
    async with httpx.AsyncClient(timeout=60) as c:
        for q in qs[:n]:
            t0 = time.perf_counter()
            r = (await c.post(f"{url}/search/vectors", json=payload(q, k))).json()
            b = r["nodes"]["node-b"]
            out.append({"t_s": round(t0 - start, 1), "ms": round((time.perf_counter() - t0) * 1000, 1),
                        "partial": r["partial"], "node-b": b["status"] + (" (thử)" if b.get("trial") else "")})
            await asyncio.sleep(gap)
    return out


def fmt_pct(x: float) -> str:
    return f"{x * 100:.1f}"


def report(res: dict) -> str:
    L = ["# Thí nghiệm tìm kiếm phân tán", "",
         f"Bộ mẫu, {res['queries']['image']} truy vấn ảnh khác góc + {res['queries']['text']} truy vấn văn bản, k = {res['k']}. "
         "Vector truy vấn tính một lần rồi dùng chung cho mọi cấu hình (endpoint `/search/vectors`), nên mọi khác biệt "
         "chỉ đến từ phần phân tán. Cache coordinator tắt.", "",
         "## E1. Phân tán có làm thay đổi kết quả?", "",
         "| Topology | Truy vấn | Cùng top-k | Điểm trùng từng bit | Lệch điểm lớn nhất | R@1 ảnh | R@10 ảnh |",
         "|---|---|---|---|---|---|---|"]
    c = res["central"]
    L.append(f"| tập trung (1 chỉ mục, trong tiến trình) | {res['queries']['total']} | — | — | — | {fmt_pct(c['R@1'])} | {fmt_pct(c['R@10'])} |")
    for t, v in res["topologies"].items():
        e = v["exact"]
        L.append(f"| {t} ({len(TOPOLOGIES[t])} node) | {e['queries']} | {e['same_top_k']} | {e['bitwise_same_scores']} | "
                 f"{e['max_abs_score_diff']:.1e} | {fmt_pct(v['recall']['R@1'])} | {fmt_pct(v['recall']['R@10'])} |")
    L += ["", "## E2. Độ trễ và thông lượng (chỉ phần tìm kiếm, không gồm embedding)", "",
          "| Topology | Đồng thời | QPS | Client p50 / p95 / p99 (ms) | Fan-out p50 (ms) | Tìm trong node p50 (ms) |",
          "|---|---|---|---|---|---|"]
    for t, v in res["topologies"].items():
        for lt in v["latency"]:
            cm = lt["client_ms"]
            L.append(f"| {t} | {lt['concurrency']} | {lt['qps']:.0f} | {cm['p50']:.1f} / {cm['p95']:.1f} / {cm['p99']:.1f} | "
                     f"{lt['fanout_ms']['p50']:.1f} | {lt['node_ms']['p50']:.2f} |")
    f = res.get("failure")
    if f:
        L += ["", "## E3. Chịu lỗi (n3, timeout 2 s; thử lại 1 lần chỉ khi lỗi kết nối; cầu dao mở 10 s sau 3 lỗi liên tiếp, health check qua thì nửa mở cho 1 truy vấn thử)", "",
              "| Kịch bản | R@1 ảnh | R@10 ảnh | Tỉ lệ kết quả thiếu node |", "|---|---|---|---|"]
        for name, v in f["recall"].items():
            L.append(f"| {name} | {fmt_pct(v['R@1'])} | {fmt_pct(v['R@10'])} | {fmt_pct(v['partial'])} |")
        L += ["", f"Truy vấn có sản phẩm đích nằm ở node-b (zara, uniqlo): {fmt_pct(f['share_b'])}% số truy vấn ảnh — "
                  "đó là phần recall mất đi khi node-b chết.", "",
              "Node-b chậm 3 s (quá timeout 2 s): truy vấn tuần tự cách nhau 1 s", "",
              "| # | t (s) | ms | node-b | thiếu node |", "|---|---|---|---|---|"]
        for i, s in enumerate(f["slow"], 1):
            L.append(f"| {i} | {s['t_s']} | {s['ms']} | {s['node-b']} | {s['partial']} |")
    return "\n".join(L) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run.py search bench", description="Thí nghiệm phân tán: chính xác, độ trễ, chịu lỗi")
    ap.add_argument("--topologies", nargs="*", choices=list(TOPOLOGIES), default=list(TOPOLOGIES),
                    help="để trống (--topologies) thì bỏ E1/E2, chỉ chạy E3")
    ap.add_argument("-k", type=int, default=10)
    ap.add_argument("--text", type=int, default=100, help="số truy vấn văn bản")
    ap.add_argument("--report", help="ghép kết quả E1/E2 từ metrics.json của lần chạy trước (khi chỉ chạy lại E3)")
    ap.add_argument("--load", type=int, default=600, help="số yêu cầu mỗi mức đồng thời")
    ap.add_argument("--concurrency", nargs="*", type=int, default=[1, 16])
    ap.add_argument("--no-failure", action="store_true")
    a = ap.parse_args(argv)

    shard = Shard()
    qs = build_queries(QueryEncoder(dets=shard.dets), shard, a.text)
    central = [shard.rank(q["kind"], q["Q"], q["qregions"], a.k, None, q["exclude_sha"]) for q in qs]
    res = {"k": a.k, "queries": {"image": sum(q["kind"] == "image" for q in qs), "text": sum(q["kind"] == "text" for q in qs),
                                 "total": len(qs)},
           "central": recall(qs, [{"results": c} for c in central]), "topologies": {}}
    if a.report:
        res["topologies"] = json.loads(Path(a.report).read_text(encoding="utf-8"))["topologies"]
    for t in a.topologies:
        with Cluster(t) as cl:
            dist = asyncio.run(run_all(cl.url, qs, a.k))
            res["topologies"][t] = {"exact": exactness(qs, central, dist), "recall": recall(qs, dist),
                                    "latency": [asyncio.run(latency(cl.url, qs, a.k, a.load, c)) for c in a.concurrency]}
            log.info("%s: %s", t, res["topologies"][t]["exact"])
    if not a.no_failure:
        img = [q for q in qs if q["kind"] == "image"]
        b_sources = set(TOPOLOGIES["n3"]["node-b"][1])
        src_of = {p["product_key"]: p["source"] for p in shard.products}
        fail = {"recall": {}, "share_b": float(np.mean([src_of[q["target"]] in b_sources for q in img]))}
        with Cluster("n3") as cl:
            r = asyncio.run(run_all(cl.url, img, a.k))
            fail["recall"]["đủ 3 node"] = {**recall(img, r), "partial": float(np.mean([x["partial"] for x in r]))}
            compose("stop", "node-b")
            r = asyncio.run(run_all(cl.url, img, a.k))
            fail["recall"]["node-b chết"] = {**recall(img, r), "partial": float(np.mean([x["partial"] for x in r]))}
            compose("--profile", "n3", "up", "-d", "--wait", "node-b", env={"NODE_B_DELAY_MS": "3000"})
            time.sleep(6)  # chờ health check đóng cầu dao
            fail["slow"] = asyncio.run(sequential(cl.url, img, a.k, 16, 1.0))
        res["failure"] = fail
    run = REPORT_DIR / f"distributed_{datetime.now():%Y%m%d_%H%M%S}"
    run.mkdir(parents=True, exist_ok=True)
    (run / "metrics.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    md = report(res)
    (run / "report.md").write_text(md, encoding="utf-8")
    print(md)
    log.info("kết quả: %s", run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
