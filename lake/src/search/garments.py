import pyarrow.parquet as pq
from PIL import Image

from src.search.catalog import DETECTIONS

VOCAB = "shirt. t-shirt. sweater. jacket. coat. trousers. jeans. shorts. skirt. dress. shoes. sneakers. sandals. boots."
# Nhãn Grounding DINO hay gộp cụm ("sweater jacket coat"), nên gom theo vị trí trên cơ thể thay vì theo từng loại.
GROUPS = {
    "upper": ("shirt", "sweater", "jacket", "coat", "dress"),
    "lower": ("trousers", "jeans", "shorts", "skirt"),
    "feet": ("shoes", "sneakers", "sandals", "boots"),
}
L1_GROUP = {"tops": "upper", "outerwear": "upper", "bottoms": "lower", "footwear": "feet"}
THRESHOLD, MIN_AREA, MAX_AREA, PAD = 0.3, 0.02, 0.9, 0.05


def group_of(label: str) -> str | None:
    # Nhãn gộp nhiều nhóm ("jacket trousers jeans") lấy nhóm có nhiều từ khớp nhất.
    words = label.replace(" - ", "-").split()
    hits = {g: sum(any(k in w for k in kw) for w in words) for g, kw in GROUPS.items()}  # "t-shirt" chứa "shirt"
    best = max(hits, key=hits.get)
    return best if hits[best] else None


def load_detections() -> dict[str, list[dict]]:
    p = DETECTIONS / "data.parquet"
    have: dict[str, list[dict]] = {}
    if p.is_file():
        for r in pq.read_table(p).to_pylist():
            have.setdefault(r["sha256"], [])
            if r["label"] is not None:
                have[r["sha256"]].append(r)
    return have


def regions(dets: list[dict]) -> dict[str, tuple[float, float, float, float]]:
    """Mỗi nhóm một khung = hợp các khung cùng nhóm (đôi giày là hai khung); bỏ khung quá nhỏ hoặc gần cả ảnh."""
    out = {}
    for g in GROUPS:
        bs = [d for d in dets if group_of(d["label"]) == g]
        if not bs:
            continue
        box = (min(d["x0"] for d in bs), min(d["y0"] for d in bs), max(d["x1"] for d in bs), max(d["y1"] for d in bs))
        area = (box[2] - box[0]) * (box[3] - box[1])
        if MIN_AREA <= area <= MAX_AREA:
            out[g] = box
    return out


def crop_image(im: Image.Image, box: tuple[float, float, float, float]) -> Image.Image:
    w, h = im.size
    x0, y0, x1, y1 = box
    px, py = PAD * (x1 - x0), PAD * (y1 - y0)
    return im.crop((max(0, (x0 - px) * w), max(0, (y0 - py) * h), min(w, (x1 + px) * w), min(h, (y1 + py) * h)))
