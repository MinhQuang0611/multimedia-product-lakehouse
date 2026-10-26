import argparse
import json
import time
from pathlib import Path

import numpy as np
from PIL import Image

from lib.config import get_logger
from src.search.catalog import SAMPLE, load_rgb
from src.search.crops import Detector
from src.search.encoders import Encoder
from src.search.garments import GROUPS, crop_image, load_detections, regions
from src.search.shard import Shard

log = get_logger("search.engine")
REGIONS = ("full", *GROUPS)


def ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000, 2)


class QueryEncoder:
    """Phía truy vấn (cần torch, GPU nếu có): ảnh → ảnh gốc + vùng món đồ → vector; văn bản → vector.

    `dets` là cache khung đã phát hiện theo sha256 (ảnh có trong kho thì không phát hiện lại)."""

    def __init__(self, image_model: str = "fashion_siglip", text_model: str = "siglip2_b16", crops: bool = True,
                 dets: dict | None = None):
        self.models = {"image": image_model, "text": text_model}
        self.crops = crops
        self.dets = load_detections() if crops and dets is None else (dets or {})
        self.enc: dict[str, Encoder] = {}
        self.detector: Detector | None = None

    def encoder(self, kind: str) -> Encoder:
        if kind not in self.enc:
            self.enc[kind] = Encoder(self.models[kind])
        return self.enc[kind]

    def warmup(self) -> None:
        self.encoder("image")
        self.encoder("text")
        if self.crops:
            self.detector = self.detector or Detector()

    def image(self, im: Image.Image, region: str | None = None, sha: str | None = None) -> dict:
        """`region` chỉ dùng một vùng ("full", "upper", "lower", "feet"); mặc định ảnh gốc + mọi vùng phát hiện được."""
        enc = self.encoder("image")  # nạp model trước khi bấm giờ: lần đầu mất vài giây
        t, boxes = {}, {}
        t0 = time.perf_counter()
        if self.crops and region != "full":
            dets = self.dets.get(sha) if sha else None
            if dets is None:
                self.detector = self.detector or Detector()
                t0 = time.perf_counter()
                dets = self.detector([im])[0]
            boxes = regions(dets)
        t["detect_ms"] = ms(t0)
        parts = {"full": im, **{g: crop_image(im, b) for g, b in boxes.items()}}
        if region:
            if region not in parts:
                raise ValueError(f"không có vùng {region}; phát hiện được: {list(parts)}")
            parts = {region: parts[region]}
        t1 = time.perf_counter()
        Q = enc.images(list(parts.values()))
        t["embed_ms"] = ms(t1)
        return {"Q": Q, "qregions": list(parts), "regions": boxes, "timing": t, "version": enc.version}

    def text(self, text: str) -> dict:
        enc = self.encoder("text")
        t0 = time.perf_counter()
        Q = enc.texts([text])
        return {"Q": Q, "qregions": ["text"], "regions": {}, "timing": {"embed_ms": ms(t0)}, "version": enc.version}


class SearchEngine:
    """Tìm kiếm tập trung: một Shard chứa mọi nguồn + QueryEncoder trong cùng tiến trình."""

    def __init__(self, data: Path = SAMPLE, image_model: str = "fashion_siglip", text_model: str = "siglip2_b16",
                 crops: bool = True, sources: list[str] | None = None, text: bool = True):
        self.shard = Shard(data, image_model, text_model, crops, sources, text)
        self.query = QueryEncoder(image_model, text_model, crops, self.shard.dets)

    def search_image(self, im: Image.Image, k: int = 10, filters: dict | None = None, region: str | None = None,
                     sha: str | None = None, exclude_sha: str | None = None) -> dict:
        q = self.query.image(im, region, sha)
        t0 = time.perf_counter()
        hits = self.shard.rank("image", q["Q"], q["qregions"], k, filters, exclude_sha)
        return {"regions": q["regions"], "results": hits, "timing": {**q["timing"], "search_ms": ms(t0)}}

    def search_text(self, text: str, k: int = 10, filters: dict | None = None) -> dict:
        q = self.query.text(text)
        t0 = time.perf_counter()
        hits = self.shard.rank("text", q["Q"], q["qregions"], k, filters)
        return {"results": hits, "timing": {**q["timing"], "search_ms": ms(t0)}}


def selfcheck(eng: SearchEngine) -> dict:
    # Chạy lại truy vấn khác góc của thí nghiệm crops qua đúng đường tìm kiếm thật (FAISS, bỏ chính ảnh truy vấn).
    # Khác evaluate một chút: exclude_sha bỏ ảnh đó ở mọi sản phẩm (đúng nghĩa khi tìm thật), còn cross_view chỉ bỏ
    # trong sản phẩm của truy vấn; bộ mẫu có một ảnh dùng chung cho hai sản phẩm Uniqlo nên lệch được tối đa vài truy vấn.
    shas = {}
    for r in eng.shard.media:
        shas.setdefault(r["product_key"], set()).add(r["sha256"])
    queries = [r for r in eng.shard.media if len(shas[r["product_key"]]) >= 2]
    ranks = []
    for r in queries:
        res = eng.search_image(load_rgb(r["path"]), k=10, sha=r["sha256"], exclude_sha=r["sha256"])["results"]
        keys = [h["product_key"] for h in res]
        ranks.append(keys.index(r["product_key"]) + 1 if r["product_key"] in keys else 11)
    ranks = np.array(ranks)
    return {"queries": len(ranks), "R@1": float((ranks == 1).mean()), "R@10": float((ranks <= 10).mean())}


def add_filter_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("-k", type=int, default=10)
    ap.add_argument("--region", choices=REGIONS)
    ap.add_argument("--source", nargs="*")
    ap.add_argument("--category-l1", nargs="*")
    ap.add_argument("--category-l2", nargs="*")
    ap.add_argument("--audience", nargs="*")
    ap.add_argument("--price-min", type=float)
    ap.add_argument("--price-max", type=float)
    ap.add_argument("--in-stock", action="store_true")


def filters_of(a: argparse.Namespace) -> dict:
    return {"source": a.source, "category_l1": a.category_l1, "category_l2": a.category_l2, "audience": a.audience,
            "price_min": a.price_min, "price_max": a.price_max, "in_stock": a.in_stock}


def print_results(res: dict) -> None:
    if res.get("regions"):
        print("vùng phát hiện:", ", ".join(f"{g} {tuple(round(x, 2) for x in b)}" for g, b in res["regions"].items()))
    print("thời gian (ms):", res["timing"])
    for i, h in enumerate(res["results"], 1):
        price = f"{h['price_vnd']:,.0f}đ" if h["price_vnd"] else "—"
        print(f"{i:>2}. {h['score']:.3f}  {h['source']:<7} {h['category_l2'] or '':<16} {price:>12}  "
              f"[{h['query_region']}→{h['match_region']}]  {(h['name'] or '')[:60]}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run.py search query", description="Tìm sản phẩm bằng ảnh hoặc văn bản trên bộ mẫu")
    q = ap.add_mutually_exclusive_group(required=True)
    q.add_argument("--image", type=Path)
    q.add_argument("--text")
    q.add_argument("--selfcheck", action="store_true", help="chạy lại đánh giá khác góc qua chỉ mục FAISS")
    add_filter_args(ap)
    ap.add_argument("--no-crops", action="store_true", help="chỉ dùng ảnh gốc (không phát hiện món đồ)")
    ap.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    eng = SearchEngine(crops=not a.no_crops, text=a.text is not None)
    if a.selfcheck:
        print(json.dumps(selfcheck(eng), indent=1))
        return 0
    res = (eng.search_image(load_rgb(a.image), a.k, filters_of(a), a.region) if a.image
           else eng.search_text(a.text, a.k, filters_of(a)))
    if a.json:
        print(json.dumps(res, ensure_ascii=False, indent=1, default=str))
    else:
        print_results(res)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
