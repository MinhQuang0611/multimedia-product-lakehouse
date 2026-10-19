from __future__ import annotations

from src.common.config import get_logger, paths
from src.common.lake import scan
from src.silver.quality import Rule, split_valid

log = get_logger("silver.shopee")


def build_listing(con) -> dict:
    staged = f"""
        SELECT trim(posting_id)  AS posting_id,
               trim(image)       AS image,
               trim(image_phash) AS image_phash,
               trim(title)       AS title,
               try_cast(label_group AS BIGINT) AS label_group,
               _ingest_ts, _batch_id
        FROM {scan(paths().bronze / 'shopee' / 'train')}
        QUALIFY row_number() OVER (PARTITION BY trim(posting_id) ORDER BY _ingest_ts DESC) = 1
    """
    rules = [
        Rule("SP_IMAGE", "image IS NULL OR NOT ends_with(lower(image), '.jpg')"),
        Rule("SP_LABEL", "label_group IS NULL"),
    ]
    return split_valid(con, staged, rules, "shopee/listing")


def run(con) -> None:
    log.info("Silver Shopee")
    build_listing(con)
