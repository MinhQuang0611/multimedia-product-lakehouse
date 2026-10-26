import argparse
import json
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
from PIL import Image, ImageEnhance, ImageFilter

from lib.config import get_logger
from src.search.catalog import REPORT_DIR, SAMPLE, load_media, load_rgb, register_views
from src.search.embed import ensure
from src.search.encoders import MODELS, Encoder

log = get_logger("search.eval")
KS = (1, 5, 10)
SEED = 20261026


class Gallery:
    """Ma trận ảnh của bộ mẫu (thứ tự theo sản phẩm) cùng chỉ số để gộp điểm ảnh về sản phẩm."""

    def __init__(self, media: list[dict]):
        self.media = media
        self.prod = np.array([r["product_key"] for r in media])
        self.sha = np.array([r["sha256"] for r in media])
        # load_media sắp theo product_key nên ảnh của một sản phẩm nằm liền nhau: gộp bằng reduceat.
        self.starts = np.flatnonzero(np.r_[True, self.prod[1:] != self.prod[:-1]])
        self.products = self.prod[self.starts]
        self.pidx = np.cumsum(np.r_[True, self.prod[1:] != self.prod[:-1]]) - 1  # ảnh → chỉ số sản phẩm
        first = {p: i for i, p in zip(self.starts, self.products)}
        self.p_cat2 = np.array([media[first[p]]["category_l2"] for p in self.products])
        self.p_source = np.array([media[first[p]]["source"] for p in self.products])
        n_img = np.diff(np.r_[self.starts, len(media)])
        # Truy vấn khác góc: ảnh của sản phẩm có ≥2 ảnh khác nhau (brands, tiki, uniqlo, zara).
        n_sha = defaultdict(set)
        for r in media:
            n_sha[r["product_key"]].add(r["sha256"])
        self.queries = np.array([i for i, r in enumerate(media) if len(n_sha[r["product_key"]]) >= 2])
        self.n_img = n_img

    def matrix(self, vecs: dict[str, np.ndarray]) -> np.ndarray:
        return np.stack([vecs[s] for s in self.sha])

    def product_scores(self, s: np.ndarray) -> np.ndarray:
        return np.maximum.reduceat(s, self.starts, axis=1)


def rank_of(scores: np.ndarray, target: np.ndarray) -> np.ndarray:
    # Hạng (từ 1) của cột target trong từng dòng; hoà điểm tính lạc quan.
    t = scores[np.arange(len(target)), target]
    return 1 + (scores > t[:, None]).sum(1)


def cross_view(g: Gallery, G: np.ndarray, s: np.ndarray | None = None) -> dict[str, np.ndarray]:
    """Chỉ số theo từng truy vấn khi bỏ chính ảnh truy vấn khỏi kho: phải tìm ra ảnh khác của cùng sản phẩm.

    `s` (truy vấn × ảnh) cho phép truyền điểm tính sẵn, ví dụ khi mỗi ảnh có nhiều vector (ảnh gốc + vùng cắt)."""
    q = g.queries
    s = G[q] @ G.T if s is None else s.copy()
    same = g.prod[q][:, None] == g.prod[None, :]
    s[same & (g.sha[q][:, None] == g.sha[None, :])] = -np.inf  # chính nó và bản trùng trong cùng sản phẩm
    rel = same & np.isfinite(s)
    order = np.argsort(-s, axis=1)
    hits = np.take_along_axis(rel, order, axis=1)
    prec = np.cumsum(hits, 1) / np.arange(1, hits.shape[1] + 1)
    ap = (prec * hits).sum(1) / hits.sum(1)
    ps = g.product_scores(s)
    own = g.pidx[q]
    rank = rank_of(ps, own)
    ps[np.arange(len(q)), own] = -np.inf
    k = min(10, ps.shape[1] - 1)
    top = np.argpartition(-ps, k - 1, axis=1)[:, :k]
    cat = (g.p_cat2[top] == g.p_cat2[own][:, None]).mean(1)
    out = {"ap": ap, "rank": rank, "cat_p10": cat}
    for k in KS:
        out[f"img_r{k}"] = hits[:, :k].any(1).astype(float)
    return out


def summarize(m: dict[str, np.ndarray], sel: np.ndarray | None = None) -> dict[str, float]:
    sel = np.ones(len(m["rank"]), bool) if sel is None else sel
    r = m["rank"][sel]
    out = {f"R@{k}": float((r <= k).mean()) for k in KS}
    out["MRR"] = float((1 / r).mean())
    out["mAP"] = float(m["ap"][sel].mean())
    out["cat_P@10"] = float(m["cat_p10"][sel].mean())
    out["n"] = int(sel.sum())
    return out


def bootstrap(groups: np.ndarray, values: dict[str, np.ndarray], reps: int = 2000) -> dict[str, list[float]]:
    """Khoảng tin cậy 95% bootstrap theo cụm sản phẩm (các ảnh của một sản phẩm không độc lập với nhau)."""
    rng = np.random.default_rng(SEED)
    uniq, inv = np.unique(groups, return_inverse=True)
    cnt = np.bincount(inv)
    sums = {k: np.bincount(inv, v) for k, v in values.items()}
    draws = rng.integers(0, len(uniq), (reps, len(uniq)))
    w = np.stack([np.bincount(d, minlength=len(uniq)) for d in draws])
    n = w @ cnt
    return {k: [float(x) for x in np.percentile((w @ s) / n, [2.5, 97.5])] for k, s in sums.items()}


# Biến dạng mô phỏng ảnh người dùng tự chụp/chụp màn hình; tham số cố định theo seed để các model nhận cùng ảnh.
def aug_crop(im, rng):
    w, h = im.size
    f = np.sqrt(rng.uniform(0.55, 0.8))
    cw, ch = int(w * f), int(h * f)
    x, y = rng.integers(0, w - cw + 1), rng.integers(0, h - ch + 1)
    return im.crop((x, y, x + cw, y + ch))


def aug_rotate(im, rng):
    return im.rotate(rng.uniform(-15, 15), resample=Image.BICUBIC, fillcolor=(255, 255, 255))


def aug_jpeg(im, rng):
    from io import BytesIO

    b = BytesIO()
    im.save(b, "JPEG", quality=15)
    return Image.open(BytesIO(b.getvalue())).convert("RGB")


def aug_lowres(im, rng):
    w, h = im.size
    s = 64 / min(w, h)
    return im.resize((max(1, int(w * s)), max(1, int(h * s))), Image.BILINEAR).resize((w, h), Image.BILINEAR)


def aug_color(im, rng):
    for e in (ImageEnhance.Brightness, ImageEnhance.Contrast, ImageEnhance.Color):
        im = e(im).enhance(rng.uniform(0.6, 1.4))
    return im


def aug_recapture(im, rng):
    # Chụp lại màn hình: đặt ảnh lên nền tối, phối cảnh lệch, mờ nhẹ, sai màu, nén.
    w, h = im.size
    pad = int(0.12 * max(w, h))
    canvas = Image.new("RGB", (w + 2 * pad, h + 2 * pad), (30, 30, 34))
    canvas.paste(im, (pad, pad))
    W, H = canvas.size
    d = lambda: rng.uniform(-0.06, 0.06)
    quad = [W * d(), H * d(), W * d(), H * (1 + d()), W * (1 + d()), H * (1 + d()), W * (1 + d()), H * d()]
    im = canvas.transform((W, H), Image.QUAD, quad, Image.BICUBIC)
    im = im.filter(ImageFilter.GaussianBlur(1.2))
    return aug_jpeg(aug_color(im, rng), rng)


AUGS = {"crop": aug_crop, "rotate": aug_rotate, "jpeg": aug_jpeg, "lowres": aug_lowres, "color": aug_color,
        "recapture": aug_recapture}


def base_images(media: list[dict], side: int = 512) -> list[Image.Image]:
    # Thu nhỏ một lần (model chỉ nhìn 224 px) để tạo biến dạng nhanh và không giữ ảnh 4000 px trong RAM.
    def one(r):
        im = load_rgb(r["path"])
        im.thumbnail((side, side))
        return im

    with ThreadPoolExecutor(8) as ex:
        return list(ex.map(one, media))


def robustness(enc: Encoder, g: Gallery, G: np.ndarray, bases: list[Image.Image], batch: int) -> dict:
    """Mọi ảnh làm truy vấn sau khi biến dạng; ảnh gốc vẫn ở trong kho; đúng khi sản phẩm của nó đứng đầu."""
    out = {}
    own = g.pidx
    for name, fn in AUGS.items():
        ims = [fn(im, np.random.default_rng([SEED, i])) for i, im in enumerate(bases)]
        Q = np.concatenate([enc.images(ims[i:i + batch]) for i in range(0, len(ims), batch)])
        rank = rank_of(g.product_scores(Q @ G.T), own)
        out[name] = {f"R@{k}": float((rank <= k).mean()) for k in (1, 10)}
    return out


def text_search(enc: Encoder, g: Gallery, G: np.ndarray, data: Path) -> dict | None:
    """Tên sản phẩm (vi/en/id tuỳ nguồn) làm truy vấn văn bản; đúng khi sản phẩm đó đứng trong top-k."""
    if not enc.has_text:
        return None
    con = duckdb.connect()
    names = dict(con.execute(f"SELECT product_key, name FROM read_parquet('{(data / 'dim_product.parquet').as_posix()}')").fetchall())
    keep = [i for i, p in enumerate(g.products) if names.get(p)]
    T = np.concatenate([enc.texts([names[g.products[i]] for i in keep[j:j + 64]]) for j in range(0, len(keep), 64)])
    rank = rank_of(g.product_scores(T @ G.T), np.array(keep))
    src = g.p_source[keep]
    res = {"all": {f"R@{k}": float((rank <= k).mean()) for k in (1, 10)}}
    for s in sorted(set(src)):
        res[s] = {f"R@{k}": float((rank[src == s] <= k).mean()) for k in (1, 10)}
    return res


def latency(enc: Encoder, bases: list[Image.Image], n: int = 50) -> dict:
    # Một truy vấn trực tuyến = một ảnh (batch 1): đo từ ảnh đã giải mã tới vector.
    enc.images(bases[:1])
    ts = []
    for im in bases[:n]:
        t0 = time.perf_counter()
        enc.images([im])
        ts.append((time.perf_counter() - t0) * 1000)
    return {"p50_ms": float(np.percentile(ts, 50)), "p95_ms": float(np.percentile(ts, 95))}


def fmt(x: float) -> str:
    return f"{x * 100:.1f}"


def report(res: dict, g: Gallery) -> str:
    models = list(res)
    best = max(models, key=lambda m: res[m]["cross_view"]["all"]["R@1"])
    L = [f"# Đánh giá tìm kiếm ảnh — bộ mẫu ({len(g.media)} ảnh, {len(g.products)} sản phẩm)", "",
         f"Truy vấn khác góc: {len(g.queries)} ảnh của {len(set(g.prod[g.queries]))} sản phẩm có ≥2 ảnh "
         "(brands, tiki, uniqlo, zara); H&M và Shopee chỉ nằm trong kho làm nhiễu. Số liệu là %.", "",
         "## 1. Truy vấn khác góc (chỉ số chính)", "",
         "Bỏ chính ảnh truy vấn khỏi kho. R@k/MRR: sản phẩm đúng (điểm = ảnh giống nhất của sản phẩm) trong top-k; "
         "mAP: trên ảnh; cat_P@10: tỉ lệ 10 sản phẩm khác đầu tiên cùng `category_l2`. Ngoặc: KTC 95% bootstrap theo sản phẩm.", "",
         "| Model | dim | R@1 | R@5 | R@10 | MRR | mAP | cat_P@10 |", "|---|---|---|---|---|---|---|---|"]
    for m in models:
        a, ci = res[m]["cross_view"]["all"], res[m]["cross_view"]["ci"]
        L.append(f"| {m}{' ★' if m == best else ''} | {res[m]['dim']} | {fmt(a['R@1'])} ({fmt(ci['R@1'][0])}–{fmt(ci['R@1'][1])}) "
                 f"| {fmt(a['R@5'])} | {fmt(a['R@10'])} | {fmt(a['MRR'])} | {fmt(a['mAP'])} ({fmt(ci['mAP'][0])}–{fmt(ci['mAP'][1])}) "
                 f"| {fmt(a['cat_P@10'])} |")
    if "paired" in res[best]:
        p = res[best]["paired"]
        L += ["", f"Chênh R@1 giữa {best} và {p['vs']}: {fmt(p['diff'])} điểm, KTC 95% "
                  f"[{fmt(p['ci'][0])}, {fmt(p['ci'][1])}] (bootstrap ghép cặp trên cùng truy vấn)."]
    L += ["", "### Theo nguồn của truy vấn (R@1 sản phẩm)", "", "| Model | " + " | ".join(res[best]["cross_view"]["by_source"]) + " |",
          "|---|" + "---|" * len(res[best]["cross_view"]["by_source"])]
    for m in models:
        L.append(f"| {m} | " + " | ".join(fmt(v["R@1"]) for v in res[m]["cross_view"]["by_source"].values()) + " |")
    L += ["", "### Theo vai trò ảnh truy vấn (R@1 sản phẩm)", "", "| Model | main | gallery |", "|---|---|---|"]
    for m in models:
        r = res[m]["cross_view"]["by_role"]
        L.append(f"| {m} | {fmt(r['main']['R@1'])} | {fmt(r['gallery']['R@1'])} |")
    augs = list(AUGS)
    L += ["", "## 2. Độ bền với ảnh bị biến dạng", "",
          f"Cả {len(g.media)} ảnh làm truy vấn sau khi biến dạng, ảnh gốc vẫn trong kho. R@1 sản phẩm.", "",
          "| Model | " + " | ".join(augs) + " |", "|---|" + "---|" * len(augs)]
    for m in models:
        L.append(f"| {m} | " + " | ".join(fmt(res[m]["robustness"][a]["R@1"]) for a in augs) + " |")
    tm = [m for m in models if res[m]["text"]]
    if tm:
        cols = list(res[tm[0]]["text"])
        L += ["", "## 3. Văn bản → ảnh (tên sản phẩm làm truy vấn, R@10 sản phẩm)", "",
              "Tên tiếng Việt (tiki, brands, zara), Anh (uniqlo, hm), Indonesia (shopee).", "",
              "| Model | " + " | ".join(cols) + " |", "|---|" + "---|" * len(cols)]
        for m in tm:
            L.append(f"| {m} | " + " | ".join(fmt(res[m]["text"][c]["R@10"]) for c in cols) + " |")
    L += ["", "## 4. Chi phí", "", "| Model | dim | ms/ảnh truy vấn (p50 / p95, GPU, batch 1) | Kho vector (KB) |", "|---|---|---|---|"]
    for m in models:
        lt = res[m]["latency"]
        L.append(f"| {m} | {res[m]['dim']} | {lt['p50_ms']:.1f} / {lt['p95_ms']:.1f} | {res[m]['dim'] * 4 * len(g.media) // 1024} |")
    return "\n".join(L) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run.py search eval", description="So sánh model trên bộ mẫu: khác góc, biến dạng, văn bản")
    ap.add_argument("--models", nargs="*", choices=list(MODELS), default=list(MODELS))
    ap.add_argument("--data", type=Path, default=SAMPLE)
    ap.add_argument("--batch", type=int, default=64)
    a = ap.parse_args(argv)
    media = load_media(a.data)
    g = Gallery(media)
    bases = base_images(media)
    res, per_query = {}, {}
    for m in a.models:
        enc = Encoder(m)
        vecs, stats = ensure(m, media, enc, a.batch)
        G = g.matrix(vecs)
        cv = cross_view(g, G)
        per_query[m] = cv
        q = g.queries
        src = np.array([media[i]["source"] for i in q])
        role = np.array([media[i]["role"] for i in q])
        ci = bootstrap(g.prod[q], {"R@1": (cv["rank"] == 1).astype(float), "mAP": cv["ap"]})
        res[m] = {
            "version": enc.version, "dim": int(G.shape[1]), "embed": stats,
            "cross_view": {"all": summarize(cv), "ci": ci,
                           "by_source": {s: summarize(cv, src == s) for s in sorted(set(src))},
                           "by_role": {r: summarize(cv, role == r) for r in ("main", "gallery")}},
            "robustness": robustness(enc, g, G, bases, a.batch),
            "text": text_search(enc, g, G, a.data),
            "latency": latency(enc, bases),
        }
        log.info("%s: %s", m, res[m]["cross_view"]["all"])
        del enc
    ranked = sorted(res, key=lambda m: -res[m]["cross_view"]["all"]["R@1"])
    if len(ranked) > 1:
        b, s = ranked[:2]
        diff = (per_query[b]["rank"] == 1).astype(float) - (per_query[s]["rank"] == 1).astype(float)
        res[b]["paired"] = {"vs": s, "diff": float(diff.mean()),
                            "ci": bootstrap(g.prod[g.queries], {"d": diff})["d"]}
    register_views()
    run = REPORT_DIR / datetime.now().strftime("%Y%m%d_%H%M%S")
    run.mkdir(parents=True, exist_ok=True)
    (run / "metrics.json").write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
    md = report(res, g)
    (run / "report.md").write_text(md, encoding="utf-8")
    print(md)
    log.info("kết quả: %s", run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
