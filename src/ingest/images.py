from __future__ import annotations

import csv
import hashlib
import zipfile
from pathlib import PurePosixPath

import pandas as pd

from src.common.config import get_logger, load_config, paths

log = get_logger("ingest.images")


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


def extract(name: str, zip_name: str, keep: set[str], folder: str = "images") -> None:
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
    # full=True: ảnh gốc từ zip của cuộc thi → images_full/; mặc định ảnh 256px → images/.
    cfg = load_config()["images"]
    for name, fn in [("hm", hm_keep), ("shopee", shopee_keep)]:
        if only and name not in only:
            continue
        keep, stats = fn(cfg[name])
        log.info("[%s] lý do loại:\n%s", name, stats.to_string())
        if full:
            extract(name, cfg[name]["zip_full"], keep, "images_full")
        else:
            extract(name, cfg[name]["zip"], keep)


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--only", nargs="*", choices=["hm", "shopee"])
    ap.add_argument("--full", action="store_true", help="dùng ảnh gốc của cuộc thi thay vì bản 256px")
    a = ap.parse_args()
    run(a.only, a.full)
