import argparse
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from PIL import Image

from lib.config import get_logger
from src.search.catalog import (DETECTIONS, EMB_CROPS, REPORT_DIR, SAMPLE, load_media, load_rgb, read_store,
                                register_views, write_store)
from src.search.embed import check_version, ensure
from src.search.encoders import MODELS, Encoder
from src.search.evaluate import Gallery, bootstrap, cross_view, fmt
from src.search.garments import L1_GROUP, THRESHOLD, VOCAB, crop_image, load_detections, regions

log = get_logger("search.crops")
DETECTOR = "IDEA-Research/grounding-dino-tiny"


class Detector:
    """Grounding DINO với bộ từ vựng món đồ cố định; trả khung chuẩn hoá về [0, 1] cho từng ảnh."""

    def __init__(self, device: str | None = None):
        from transformers import AutoModelForZeroShotObjectDetection, AutoProcessor

        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.proc = AutoProcessor.from_pretrained(DETECTOR)
        self.model = AutoModelForZeroShotObjectDetection.from_pretrained(DETECTOR).to(self.device).eval()

    @torch.inference_mode()
    def __call__(self, ims: list[Image.Image]) -> list[list[dict]]:
        ims = [im.copy() for im in ims]
        for im in ims:
            im.thumbnail((800, 800))  # processor vẫn phóng về cạnh ngắn 800; thu nhỏ trước để bớt chép dữ liệu
        inp = self.proc(images=ims, text=[VOCAB] * len(ims), return_tensors="pt", padding=True).to(self.device)
        res = self.proc.post_process_grounded_object_detection(
            self.model(**inp), inp.input_ids, threshold=THRESHOLD, text_threshold=0.25,
            target_sizes=[im.size[::-1] for im in ims])
        out = []
        for im, r in zip(ims, res):
            w, h = im.size
            out.append([{"label": l, "score": float(sc), "x0": b[0] / w, "y0": b[1] / h, "x1": b[2] / w, "y1": b[3] / h}
                        for b, sc, l in zip(r["boxes"].tolist(), r["scores"].tolist(), r["text_labels"])])
        return out


def detect(media: list[dict], batch: int = 2) -> dict[str, list[dict]]:
    """Phát hiện món đồ trên mọi ảnh (cache theo sha256); toạ độ khung chuẩn hoá về [0, 1]."""
    # batch 8 cần ~11 GB VRAM (ảnh được processor phóng lên cạnh ngắn 800 px); GPU dùng chung nên để nhỏ.
    p = DETECTIONS / "data.parquet"
    have = load_detections()
    todo = {}
    for r in media:
        if r["sha256"] not in have:
            todo.setdefault(r["sha256"], r["path"])
    if todo:
        det = Detector()
        shas = list(todo)
        with ThreadPoolExecutor(8) as ex:
            for i in range(0, len(shas), batch):
                ims = list(ex.map(load_rgb, [todo[s] for s in shas[i:i + batch]]))
                for s, boxes in zip(shas[i:i + batch], det(ims)):
                    have[s] = [{"sha256": s, **b} for b in boxes]
                if i // batch % 20 == 0:
                    log.info("phát hiện %d/%d ảnh", min(i + batch, len(shas)), len(shas))
        rows = [x for s, xs in have.items() for x in xs] + \
               [{"sha256": s, "label": None, "score": None, "x0": None, "y0": None, "x1": None, "y1": None}
                for s, xs in have.items() if not xs]  # ảnh không thấy gì vẫn ghi để lần sau không chạy lại
        p.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), p.with_suffix(".tmp"))
        p.with_suffix(".tmp").replace(p)
    return have


def crop(path: Path, box: tuple[float, float, float, float]) -> Image.Image:
    return crop_image(load_rgb(path), box)


def crop_vectors(enc: Encoder, keys: dict[str, tuple[Path, tuple]], batch: int = 64) -> dict[str, np.ndarray]:
    # Kho riêng cho vùng cắt, khoá "<sha256>#<nhóm>", cùng định dạng kho ảnh gốc.
    store = EMB_CROPS
    vecs = read_store(store, enc.name)
    todo = [k for k in keys if k not in vecs]
    if todo:
        check_version(store, enc.name, enc.version, bool(vecs))
        with ThreadPoolExecutor(8) as ex:
            ims = list(ex.map(lambda k: crop(*keys[k]), todo))
        mat = np.concatenate([enc.images(ims[i:i + batch]) for i in range(0, len(ims), batch)])
        vecs.update(zip(todo, mat))
        write_store(store, enc.name, enc.version, vecs)
    return vecs


def multi_scores(q_sets: list[list[np.ndarray]], g_sets: list[list[np.ndarray]]) -> np.ndarray:
    """Điểm truy vấn × ảnh khi mỗi bên có nhiều vector: lấy cặp vector giống nhau nhất."""
    Q, G = np.concatenate([np.stack(x) for x in q_sets]), np.concatenate([np.stack(x) for x in g_sets])
    qs = np.r_[0, np.cumsum([len(x) for x in q_sets])[:-1]]
    gs = np.r_[0, np.cumsum([len(x) for x in g_sets])[:-1]]
    return np.maximum.reduceat(np.maximum.reduceat(Q @ G.T, gs, axis=1), qs, axis=0)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run.py search crops", description="Thí nghiệm cắt vùng món đồ (Grounding DINO)")
    ap.add_argument("--models", nargs="*", choices=list(MODELS), default=["fashion_siglip", "siglip2_b16"])
    ap.add_argument("--data", type=Path, default=SAMPLE)
    a = ap.parse_args(argv)
    media = load_media(a.data)
    g = Gallery(media)
    dets = detect(media)
    reg = [regions(dets[r["sha256"]]) for r in media]
    # Kho: vùng của nhóm khớp danh mục sản phẩm (thông tin có sẵn khi lập chỉ mục).
    # Truy vấn: không biết danh mục, dùng mọi vùng phát hiện được.
    g_keys = {i: f"{r['sha256']}#{L1_GROUP[r['category_l1']]}" for i, r in enumerate(media)
              if L1_GROUP.get(r["category_l1"]) in reg[i]}
    q_keys = {i: [f"{media[i]['sha256']}#{k}" for k in reg[i]] for i in g.queries}
    keys = {f"{r['sha256']}#{k}": (r["path"], box) for i, r in enumerate(media) for k, box in reg[i].items()}
    multi = np.array([len(reg[i]) >= 2 for i in g.queries])
    log.info("ảnh có vùng khớp danh mục: %d/%d; truy vấn nhiều món: %d/%d", len(g_keys), len(media), multi.sum(), len(multi))

    res = {}
    for m in a.models:
        enc = Encoder(m)
        G = g.matrix(ensure(m, media, enc)[0])
        C = crop_vectors(enc, keys)
        full = [[G[i]] for i in range(len(media))]
        gal_crop = [[G[i]] + ([C[g_keys[i]]] if i in g_keys else []) for i in range(len(media))]
        gal_only = [[C[g_keys[i]]] if i in g_keys else [G[i]] for i in range(len(media))]
        q_full = [[G[i]] for i in g.queries]
        q_crop = [[G[i]] + [C[k] for k in q_keys[i]] for i in g.queries]
        q_only = [[C[k] for k in q_keys[i]] or [G[i]] for i in g.queries]
        variants = {
            "A ảnh gốc / ảnh gốc": (q_full, full),
            "B kho + vùng cắt": (q_full, gal_crop),
            "C truy vấn + vùng cắt": (q_crop, full),
            "D cả hai + vùng cắt": (q_crop, gal_crop),
            "E chỉ vùng cắt": (q_only, gal_only),
        }
        res[m] = {}
        base = None
        for name, (qs, gs) in variants.items():
            s = multi_scores(qs, [gs[i] for i in range(len(media))])
            cv = cross_view(g, G, s)
            r1 = (cv["rank"] == 1).astype(float)
            base = r1 if base is None else base
            res[m][name] = {"R@1": r1.mean(), "R@10": float((cv["rank"] <= 10).mean()), "mAP": float(cv["ap"].mean()),
                            "R@1_multi": r1[multi].mean(), "R@1_single": r1[~multi].mean(),
                            "diff_ci": bootstrap(g.prod[g.queries], {"d": r1 - base})["d"]}
            log.info("%s | %s: %s", m, name, {k: v for k, v in res[m][name].items() if k != "diff_ci"})
        del enc
    L = ["# Thí nghiệm cắt vùng món đồ (Grounding DINO tiny)", "",
         f"Truy vấn khác góc như `eval`. {multi.sum()}/{len(multi)} truy vấn có ≥2 nhóm món đồ (ảnh người mặc cả bộ). "
         "Chênh R@1 so với A kèm KTC 95% bootstrap ghép cặp.", ""]
    for m, rows in res.items():
        L += [f"## {m}", "", "| Biến thể | R@1 | Chênh R@1 vs A (KTC) | R@10 | mAP | R@1 nhiều món | R@1 một món |",
              "|---|---|---|---|---|---|---|"]
        for name, v in rows.items():
            ci = v["diff_ci"]
            L.append(f"| {name} | {fmt(v['R@1'])} | {fmt(v['R@1'] - rows['A ảnh gốc / ảnh gốc']['R@1'])} "
                     f"[{fmt(ci[0])}, {fmt(ci[1])}] | {fmt(v['R@10'])} | {fmt(v['mAP'])} | {fmt(v['R@1_multi'])} | "
                     f"{fmt(v['R@1_single'])} |")
        L.append("")
    md = "\n".join(L)
    register_views()
    run = REPORT_DIR / f"crops_{datetime.now():%Y%m%d_%H%M%S}"
    run.mkdir(parents=True, exist_ok=True)
    (run / "report.md").write_text(md, encoding="utf-8")
    (run / "metrics.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    print(md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
