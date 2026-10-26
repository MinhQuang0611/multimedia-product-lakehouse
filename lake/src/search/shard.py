import time
from pathlib import Path

import faiss
import numpy as np

from lib.config import get_logger
from src.search.catalog import EMB, EMB_CROPS, SAMPLE, load_media, load_products, read_store, store_version
from src.search.garments import L1_GROUP, load_detections, regions

log = get_logger("search.shard")
FACETS = ("source", "category_l1", "category_l2", "audience")


class VectorIndex:
    """FAISS IndexFlatIP (vector đã L2-normalize nên tích vô hướng = cosine, tìm chính xác) kèm ánh xạ vector → sản phẩm."""

    def __init__(self, vecs: np.ndarray, owner: np.ndarray, region: list[str], vprod: np.ndarray, version: str | None):
        self.index = faiss.IndexFlatIP(vecs.shape[1])
        self.index.add(np.ascontiguousarray(vecs, dtype=np.float32))
        self.owner = owner  # vector → chỉ số ảnh
        self.region = np.array(region)  # "full" hoặc nhóm vùng cắt
        self.vprod = vprod  # vector → chỉ số sản phẩm
        self.version = version
        # Điểm sản phẩm = max trên các vector của nó. Vector tốt nhất của sản phẩm hạng k đứng sau tối đa (k-1)·vmax
        # vector của các sản phẩm hơn nó, nên k·vmax vector đầu là đủ để top-k sản phẩm chính xác (xem Shard.rank).
        self.vmax = int(np.bincount(vprod).max())

    def search(self, Q: np.ndarray, m: int, allowed: np.ndarray | None = None) -> tuple[np.ndarray, np.ndarray, int]:
        """m vector gần nhất cho mỗi vector truy vấn, trong tập `allowed` (nếu có); kèm số vector được phép."""
        params, avail = None, self.index.ntotal
        if allowed is not None:
            ids = np.flatnonzero(allowed).astype(np.int64)
            if len(ids) == 0:
                return np.zeros((len(Q), 0), np.float32), np.zeros((len(Q), 0), np.int64), 0
            params, avail = faiss.SearchParameters(sel=faiss.IDSelectorBatch(ids)), len(ids)
        D, I = self.index.search(np.ascontiguousarray(Q, dtype=np.float32), min(m, avail), params=params)
        return D, I, avail


class Shard:
    """Phần dữ liệu một node giữ: sản phẩm của các nguồn `sources` (None = tất cả) và chỉ mục FAISS của chúng.

    Không cần torch: nhận vector truy vấn đã tính sẵn. Nguồn sự thật là embedding Parquet trong gold; chỉ mục dựng lại
    khi khởi tạo. Ảnh → `image_model` (ảnh gốc + vùng cắt khớp danh mục), văn bản → `text_model` (ảnh gốc)."""

    def __init__(self, data: Path = SAMPLE, image_model: str = "fashion_siglip", text_model: str = "siglip2_b16",
                 crops: bool = True, sources: list[str] | None = None, text: bool = True):
        t0 = time.perf_counter()
        self.data, self.sources, self.crops = data, sources, crops
        self.models = {"image": image_model, **({"text": text_model} if text else {})}
        media = load_media(data)
        self.media = [r for r in media if sources is None or r["source"] in sources]
        if not self.media:
            raise SystemExit(f"shard {sources}: không có ảnh nào. Nguồn có ảnh: {sorted({r['source'] for r in media})}")
        keys = {r["product_key"] for r in self.media}
        self.products = [p for p in load_products(data) if p["product_key"] in keys]
        pidx = {p["product_key"]: i for i, p in enumerate(self.products)}
        self.mprod = np.array([pidx[r["product_key"]] for r in self.media])
        self.facets = {f: np.array([p[f] for p in self.products], dtype=object) for f in FACETS}
        self.price = np.array([np.nan if p["price_vnd"] is None else p["price_vnd"] for p in self.products])
        self.in_stock = np.array([bool(p["in_stock"]) for p in self.products])
        self.dets = load_detections() if crops else {}
        self.index = {kind: self.build(m, crops and kind == "image") for kind, m in self.models.items()}
        log.info("shard %s: %d sản phẩm, %s vector (%.1f s)", sources or "tất cả", len(self.products),
                 {k: v.index.ntotal for k, v in self.index.items()}, time.perf_counter() - t0)

    def build(self, model: str, crops: bool) -> VectorIndex:
        full = read_store(EMB, model)
        missing = {r["sha256"] for r in self.media} - set(full)
        if missing:
            raise SystemExit(f"{model}: thiếu embedding cho {len(missing)} ảnh. Chạy: python run.py search embed --models {model}")
        cvecs = read_store(EMB_CROPS, model) if crops else {}
        if crops and not cvecs:
            raise SystemExit(f"{model}: chưa có embedding vùng cắt. Chạy: python run.py search crops --models {model}")
        vecs, owner, region = [], [], []
        for i, r in enumerate(self.media):
            vecs.append(full[r["sha256"]])
            owner.append(i)
            region.append("full")
            # Kho chỉ thêm vùng khớp danh mục của sản phẩm (biến thể D của thí nghiệm crops).
            g = L1_GROUP.get(r["category_l1"])
            key = f"{r['sha256']}#{g}"
            if crops and g in regions(self.dets.get(r["sha256"], [])) and key in cvecs:
                vecs.append(cvecs[key])
                owner.append(i)
                region.append(g)
        owner = np.array(owner)
        return VectorIndex(np.stack(vecs), owner, region, self.mprod[owner], store_version(EMB, model))

    def info(self) -> dict:
        return {"sources": self.sources, "products": len(self.products),
                "vectors": {k: v.index.ntotal for k, v in self.index.items()},
                "models": {k: {"name": self.models[k], "version": v.version} for k, v in self.index.items()}}

    def allowed(self, filters: dict | None) -> np.ndarray | None:
        """Mặt nạ sản phẩm theo bộ lọc: danh sách giá trị cho source/category_l1/category_l2/audience, khoảng giá VND, còn hàng."""
        if not filters:
            return None
        ok = np.ones(len(self.products), bool)
        for f in FACETS:
            if v := filters.get(f):
                ok &= np.isin(self.facets[f], [v] if isinstance(v, str) else list(v))  # "zara" không bị tách thành ký tự
        if filters.get("price_min") is not None:
            ok &= self.price >= filters["price_min"]
        if filters.get("price_max") is not None:
            ok &= self.price <= filters["price_max"]
        if filters.get("in_stock"):
            ok &= self.in_stock
        return ok

    def rank(self, kind: str, Q: np.ndarray, qregions: list[str], k: int, filters: dict | None = None,
             exclude_sha: str | None = None) -> list[dict]:
        """Top-k sản phẩm của shard. Điểm giữ nguyên độ chính xác float và hoà điểm xếp theo product_key, để gộp kết quả
        nhiều shard ra đúng thứ tự như khi tìm trên một chỉ mục chung."""
        idx = self.index[kind]
        ok = self.allowed(filters)
        mask = None if ok is None else ok[idx.vprod]
        if exclude_sha:
            keep = np.array([self.media[o]["sha256"] != exclude_sha for o in idx.owner])
            mask = keep if mask is None else mask & keep
        m = k * idx.vmax
        while True:
            D, I, avail = idx.search(Q, m, mask)
            best: dict[int, tuple[float, int, int]] = {}
            for qi in range(len(Q)):
                for s, v in zip(D[qi], I[qi]):
                    if v < 0:
                        continue
                    p = int(idx.vprod[v])
                    if p not in best or s > best[p][0]:
                        best[p] = (float(s), qi, int(v))
            # k·vmax vector là đủ, trừ khi hoà điểm đúng ngưỡng: vector chưa lấy có thể bằng điểm sản phẩm thứ k (ảnh
            # trùng nhau) và thắng nhờ phá hoà theo product_key. Lấy thêm tới khi vector cuối mỗi dòng thấp hơn hẳn.
            scores = sorted((b[0] for b in best.values()), reverse=True)
            kth = scores[k - 1] if len(scores) >= k else -np.inf
            if m >= avail or D.shape[1] == 0 or bool((D[:, -1] < kth).all()):
                break
            m *= 2
        out = []
        for p, (s, qi, v) in best.items():
            m = self.media[idx.owner[v]]
            out.append({**self.products[p], "score": s, "query_region": qregions[qi], "match_region": str(idx.region[v]),
                        "match_media_key": m["media_key"], "match_image": m["path"].relative_to(self.data).as_posix()})
        return top_k(out, k)


def top_k(hits: list[dict], k: int) -> list[dict]:
    return sorted(hits, key=lambda h: (-h["score"], h["product_key"]))[:k]
