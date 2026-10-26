from datetime import datetime, timezone
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

from lib.config import ROOT, get_logger
from src.common.core import REPORTS, layer

SAMPLE = ROOT / "data" / "sample"
# Embedding là bảng đặc trưng tầng gold, khoá theo sha256 của file ảnh (kho media theo nội dung): bộ mẫu và bộ đầy đủ
# dùng chung một kho, ảnh nào đã tính thì không tính lại. Chỉ mục FAISS chỉ là bản dựng lại từ các file này.
EMB = layer("gold", "embeddings")  # model=<tên>/data.parquet: sha256 → vector ảnh gốc
EMB_CROPS = layer("gold", "embeddings_crops")  # model=<tên>/data.parquet: "<sha256>#<nhóm>" → vector vùng cắt
DETECTIONS = layer("gold", "detections", "grounding_dino_tiny")
REPORT_DIR = REPORTS / "search"
COLS = ["media_key", "product_key", "source", "role", "position", "sha256", "path", "category_l1", "category_l2",
        "audience"]


def load_media(data: Path = SAMPLE) -> list[dict]:
    """Mỗi dòng một ảnh của bộ mẫu, kèm thuộc tính sản phẩm dùng cho đánh giá và lọc."""
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT m.media_key, m.product_key, m.source, m.role, m.position, m.sha256, m.local_path,
               p.category_l1, p.category_l2, p.audience_std
        FROM read_parquet('{(data / 'fact_media.parquet').as_posix()}') m
        JOIN read_parquet('{(data / 'dim_product.parquet').as_posix()}') p USING (product_key)
        WHERE m.media_type = 'image' AND m.download_status = 'ok' AND m.local_path IS NOT NULL
        ORDER BY m.source, m.product_key, m.position, m.media_key""").fetchall()
    con.close()
    out = [dict(zip(COLS, r)) for r in rows]
    for r in out:
        r["path"] = data / r["path"]
    return out


def load_products(data: Path = SAMPLE) -> list[dict]:
    """Một dòng mỗi sản phẩm với giá của snapshot mới nhất: dùng để lọc và hiển thị kết quả tìm kiếm."""
    con = duckdb.connect()
    t = lambda name: f"read_parquet('{(data / (name + '.parquet')).as_posix()}')"
    cur = con.execute(f"""
        SELECT p.product_key, p.source, p.name, b.brand_name AS brand, p.category_l1, p.category_l2,
               p.audience_std AS audience, p.url, f.price, f.currency, f.price_vnd, f.in_stock
        FROM {t('dim_product')} p LEFT JOIN {t('dim_brand')} b USING (brand_key)
        LEFT JOIN (SELECT * FROM {t('fact_price_snapshot')}
                   QUALIFY row_number() OVER (PARTITION BY product_key ORDER BY date_key DESC) = 1) f USING (product_key)
        ORDER BY p.product_key""")
    cols = [d[0] for d in cur.description]
    out = [dict(zip(cols, r)) for r in cur.fetchall()]
    con.close()
    return out


def load_rgb(path: Path) -> Image.Image:
    # PNG trong suốt mà convert("RGB") thẳng thì nền thành đen: ghép lên nền trắng như ảnh sản phẩm thường gặp.
    im = Image.open(path)
    im.load()
    if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
        im = im.convert("RGBA")
        bg = Image.new("RGB", im.size, "white")
        bg.paste(im, mask=im.getchannel("A"))
        return bg
    return im.convert("RGB")


def store_path(root: Path, model: str) -> Path:
    return root / f"model={model}" / "data.parquet"


def read_store(root: Path, model: str) -> dict[str, np.ndarray]:
    p = store_path(root, model)
    if not p.is_file():
        return {}
    t = pq.read_table(p, columns=["sha256", "vector"])
    mat = np.asarray(t.column("vector").combine_chunks().flatten(), dtype=np.float32).reshape(t.num_rows, -1)
    return dict(zip(t.column("sha256").to_pylist(), mat))


def store_version(root: Path, model: str) -> str | None:
    p = store_path(root, model)
    if not p.is_file():
        return None
    col = pq.read_table(p, columns=["model_version"]).column("model_version")
    return col[0].as_py() if len(col) else None


def write_store(root: Path, model: str, version: str, vecs: dict[str, np.ndarray]) -> None:
    shas = sorted(vecs)
    mat = np.stack([vecs[s] for s in shas]).astype(np.float32)
    dim = mat.shape[1]
    t = pa.table({
        "sha256": shas,
        "vector": pa.FixedSizeListArray.from_arrays(pa.array(mat.ravel()), dim),
        "model_version": [version] * len(shas),
        "dim": pa.array([dim] * len(shas), pa.int32()),
        "created_at": [datetime.now(timezone.utc).isoformat(timespec="seconds")] * len(shas),
    })
    p = store_path(root, model)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    pq.write_table(t, tmp, compression="zstd")
    tmp.replace(p)


def feature_views() -> dict[str, str]:
    """SELECT cho từng bảng đặc trưng tìm kiếm ảnh trong gold, để gold.duckdb mở được.

    DuckDB đọc vector Parquet thành list FLOAT[]; ép về mảng FLOAT[dim] để dùng được array_cosine_similarity."""
    out = {}
    for prefix, root in (("feat_image_", EMB), ("feat_crop_", EMB_CROPS)):
        for p in sorted(root.glob("model=*/data.parquet")):
            dim = pq.read_schema(p).field("vector").type.list_size
            out[prefix + p.parent.name.split("=", 1)[1]] = (
                f"SELECT * REPLACE (vector::FLOAT[{dim}] AS vector) FROM read_parquet('{p.as_posix()}')")
    if (DETECTIONS / "data.parquet").is_file():
        out["fact_garment_detection"] = f"SELECT * FROM read_parquet('{(DETECTIONS / 'data.parquet').as_posix()}')"
    return out


def register_views() -> None:
    # GoldAgent dựng lại gold.duckdb mỗi lần chạy và cũng gọi feature_views(); hàm này cho lúc chỉ chạy embed.
    # gold.duckdb đang bị khoá (gold đang dựng lại, ai đó đang mở) thì chỉ cảnh báo: view sẽ có ở lần gold chạy sau.
    try:
        con = duckdb.connect(str(layer("gold", "gold.duckdb")))
    except duckdb.Error as e:
        get_logger("search").warning("không mở được gold.duckdb để thêm view (%s)", e)
        return
    for name, sql in feature_views().items():
        con.execute(f"CREATE OR REPLACE VIEW {name} AS {sql}")
    con.close()
