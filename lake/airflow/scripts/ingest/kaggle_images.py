import csv
import hashlib
import sys
import zipfile
from pathlib import Path, PurePosixPath

import pandas as pd

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from lib.config import get_logger, paths

log = get_logger("ingest.kaggle_images")

# Bỏ đồ nữ và đồ nhạy cảm cho đồng bộ với Tiki (đồ án nộp/trình bày). Đồ trẻ em giữ lại, trừ đồ lót/đồ bơi.
# zip: bản 256px (dataset công khai); zip_full: ảnh gốc của cuộc thi (cần chấp nhận luật cuộc thi).
SOURCES = {
    "hm": {
        "zip": "hm256x256.zip",  # kaggle datasets download odins0n/hm256x256
        "zip_full": "h-and-m-personalized-fashion-recommendations.zip",  # 28,7 GB
        "keep_index_groups": ["Menswear", "Baby/Children", "Sport"],
        "drop_sections": ["Ladies H&M Sport", "Men Underwear", "Boys Underwear & Basics",
                          "Girls Underwear & Basics", "Kids Accessories, Swimwear & D"],
        "drop_product_groups": ["Underwear", "Swimwear", "Nightwear", "Socks & Tights", "Underwear/nightwear"],
    },
    "shopee": {
        "zip": "shopee-training-images-256px-jpg.zip",  # kaggle datasets download xhlulu/shopee-training-images-256px-jpg
        "zip_full": "shopee-product-matching.zip",  # 1,7 GB
        # Tiêu đề tiếng Indonesia; không dùng "bh" (thường là "buah" = cái).
        "sensitive": r"\bbra\b|bralette|celana dalam|underwear|lingerie|lingeri|bikini|baju renang|swimsuit|sexy"
                     r"|seksi|kondom|g-?string|thong|korset|bustier|\bvibrat",
        "female_marker": r"wanita|cewek|perempuan|ladies|women|muslimah|hamil|menyusui",
        "apparel": r"baju|kaos|celana|kemeja|jaket|sweater|hoodie|setelan|piyama|tunik|cardigan|outer|blazer"
                   r"|kulot|legging|sepatu|sandal|\btas\b|dompet|atasan|bawahan|pakaian|busana",
        "female_garment": r"\bdress\b|gamis|\brok\b|daster|blouse|hijab|jilbab|kerudung|pashmina|mukena|khimar"
                          r"|bergo|kebaya|heels|wedges",
    },
}


def hm_keep(cfg: dict) -> tuple[set[str], pd.Series]:
    a = pd.read_csv(paths().raw / "hm" / "articles.csv", dtype={"article_id": str},
                    usecols=["article_id", "index_group_name", "section_name", "product_group_name"])
    reason = pd.Series("", index=a.index)
    reason[~a.index_group_name.isin(cfg["keep_index_groups"])] = "index_group"
    reason[a.section_name.isin(cfg["drop_sections"])] = "section"
    reason[a.product_group_name.isin(cfg["drop_product_groups"])] = "product_group"
    keep = set(a.loc[reason == "", "article_id"].str.zfill(10))
    return keep, reason[reason != ""].value_counts()


def shopee_keep(cfg: dict) -> tuple[set[str], pd.Series]:
    s = pd.read_csv(paths().raw / "shopee" / "train.csv")
    t = s.title.str.lower()
    flags = {
        "nhạy cảm": t.str.contains(cfg["sensitive"]),
        "đồ nữ": t.str.contains(cfg["female_marker"]) & t.str.contains(cfg["apparel"]),
        "đồ nữ đặc thù": t.str.contains(cfg["female_garment"]),
    }
    flagged = pd.concat(flags, axis=1).any(axis=1)
    # Bỏ cả label_group: giữ lại nửa nhóm sẽ làm sai ground truth "cùng sản phẩm".
    drop = s.label_group.isin(s.loc[flagged, "label_group"])
    stats = pd.Series({k: int(v.sum()) for k, v in flags.items()} | {"tin đăng bị loại (cả nhóm)": int(drop.sum())})
    return set(s.loc[~drop, "image"].str.removesuffix(".jpg")), stats


def extract(name: str, zip_name: str, keep: set[str], folder: str) -> None:
    out = paths().raw / name / folder
    out.mkdir(parents=True, exist_ok=True)
    rows, n_zip = [], 0
    with zipfile.ZipFile(paths().raw / "_zips" / zip_name) as zf:
        for info in zf.infolist():
            p = PurePosixPath(info.filename)
            if info.is_dir() or p.suffix.lower() != ".jpg":
                continue
            n_zip += 1
            # Zip gốc của Shopee có cả test_images (không có label) — bỏ qua.
            if p.stem not in keep or "test_images" in p.parts:
                continue
            dest = out / p.name
            data = zf.read(info) if not dest.exists() else dest.read_bytes()
            if not dest.exists():
                dest.write_bytes(data)
            rows.append({"id": p.stem, "path": p.name, "bytes": len(data), "sha1": hashlib.sha1(data).hexdigest()})
    with open(out / "_manifest.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "path", "bytes", "sha1"])
        w.writeheader()
        w.writerows(rows)
    log.info("[%s] giữ %d/%d ảnh trong zip (%d id hợp lệ, %d id không có ảnh) → %s",
             name, len(rows), n_zip, len(keep), len(keep) - len(rows), out)


def run(only: list[str] | None = None, full: bool = False) -> None:
    # full=True: ảnh gốc → images_full/; mặc định ảnh 256px → images/.
    for name, fn in [("hm", hm_keep), ("shopee", shopee_keep)]:
        if only and name not in only:
            continue
        cfg = SOURCES[name]
        keep, stats = fn(cfg)
        log.info("[%s] lý do loại:\n%s", name, stats.to_string())
        extract(name, cfg["zip_full" if full else "zip"], keep, "images_full" if full else "images")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", choices=["hm", "shopee"])
    ap.add_argument("--full", action="store_true", help="dùng ảnh gốc của cuộc thi thay vì bản 256px")
    a = ap.parse_args()
    run(a.only, a.full)
