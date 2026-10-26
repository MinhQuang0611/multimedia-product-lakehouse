import argparse
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

from lib.config import get_logger
from src.search.catalog import EMB, SAMPLE, load_media, load_rgb, read_store, register_views, store_version, write_store
from src.search.encoders import MODELS, Encoder

log = get_logger("search.embed")


def encode_paths(enc: Encoder, paths: list[Path], batch: int = 64) -> np.ndarray:
    # Giải mã ảnh ở luồng phụ để GPU không phải chờ đọc đĩa.
    out = []
    with ThreadPoolExecutor(8) as ex:
        for i in range(0, len(paths), batch):
            out.append(enc.images(list(ex.map(load_rgb, paths[i:i + batch]))))
    return np.concatenate(out) if out else np.zeros((0, 0), np.float32)


def check_version(root: Path, model: str, version: str, has_rows: bool) -> None:
    # Thêm vector vào kho đã có thì phải cùng model_version: hai phiên bản model là hai không gian khác nhau.
    old = store_version(root, model)
    if has_rows and old != version:
        raise SystemExit(f"{root.name}/model={model}: kho tính bằng {old}, model hiện tại là {version}. "
                         f"Xoá thư mục {root / f'model={model}'} để tính lại toàn bộ.")


def ensure(model: str, media: list[dict], enc: Encoder | None = None, batch: int = 64, root: Path = EMB) -> tuple[dict, dict]:
    """Bảo đảm mọi ảnh (theo sha256) có embedding của model; chỉ tính phần còn thiếu rồi ghi đè kho của model."""
    vecs = read_store(root, model)
    todo = {}
    for r in media:
        if r["sha256"] not in vecs:
            todo.setdefault(r["sha256"], r["path"])
    stats = {"model": model, "cached": len(vecs), "new": len(todo)}
    if todo:
        enc = enc or Encoder(model)
        check_version(root, model, enc.version, bool(vecs))
        t0 = time.perf_counter()
        mat = encode_paths(enc, list(todo.values()), batch)
        stats["seconds"] = round(time.perf_counter() - t0, 2)
        stats["img_per_s"] = round(len(todo) / max(stats["seconds"], 1e-9), 1)
        vecs.update(zip(todo, mat))
        write_store(root, model, enc.version, vecs)
    stats["dim"] = len(next(iter(vecs.values()))) if vecs else 0
    log.info("%s: %s", model, stats)
    return vecs, stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run.py search embed", description="Trích embedding ảnh bộ mẫu theo từng model")
    ap.add_argument("--models", nargs="*", choices=list(MODELS), default=list(MODELS))
    ap.add_argument("--data", type=Path, default=SAMPLE, help="thư mục bộ mẫu (fact_media, dim_product, images/)")
    ap.add_argument("--batch", type=int, default=64)
    a = ap.parse_args(argv)
    media = load_media(a.data)
    log.info("%d ảnh, %d sha256 khác nhau", len(media), len({r["sha256"] for r in media}))
    for m in a.models:
        ensure(m, media, batch=a.batch)
    register_views()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
