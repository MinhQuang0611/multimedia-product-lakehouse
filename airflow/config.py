from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_ROOT = Path(__file__).resolve().parent


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        env_prefix="AIRFLOW_",
        extra="ignore",
    )

    # directory
    project_dir: Path = PROJECT_ROOT
    airflow_dir: Path = AIRFLOW_ROOT
    raw_dir: Path = PROJECT_ROOT / "data" / "raw"

    @property
    def dags_dir(self) -> Path:         return self.airflow_dir / "dags"

    @property
    def scripts_dir(self) -> Path:      return self.airflow_dir / "scripts"

    @property
    def local_state_dir(self) -> Path:  return self.airflow_dir / "state"

    # tiki crawl config
    # Chỉ đồ nam: Thời trang nam, Giày dép nam, Túi nam. Đã bỏ đồ nữ (931, 1703, 976) và
    # danh mục nhạy cảm vì đây là đồ án nộp/trình bày.
    crawl_tiki_roots: list[int] = [915, 1686, 27616]
    crawl_tiki_exclude: list[int] = [27548, 16004]  # Đồ lót nam, Đồ bơi - Đồ đi biển nam
    crawl_tiki_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/128 Safari/537.36"
    )
    crawl_tiki_min_interval: float = 1.0
    crawl_tiki_listing_interval: float = 3.0
    crawl_tiki_listing_burst: int = 20
    crawl_tiki_listing_pause: int = 900
    crawl_tiki_block_wait: int = 1200
    crawl_tiki_max_blocks: int = 3
    crawl_tiki_blocked_cooldown: int = 3600  # --forever: bị chặn quá max_blocks lần thì nghỉ rồi chạy lại
    crawl_tiki_detail_refresh_days: int = 7
    crawl_tiki_seller_refresh_days: int = 7
    crawl_tiki_review_products: int = 2000
    crawl_tiki_review_pages: int = 5
    crawl_tiki_image_refresh_days: int = 30

    def tiki_crawl_config(self) -> dict:
        prefix = "crawl_tiki_"
        return {name.removeprefix(prefix): value for name, value in self.model_dump().items()
                if name.startswith(prefix)}

    # datalake config
    lake_backend: Literal["s3", "local"] = "s3"
    lake_local_root: Path = PROJECT_ROOT / "data" / "lake"
    lake_s3_endpoint: str = Field(
        default="127.0.0.1:9000",
        validation_alias=AliasChoices("AIRFLOW_LAKE_S3_ENDPOINT", "LAKE_S3_ENDPOINT"),
    )
    lake_s3_bucket: str = "lake"
    lake_s3_region: str = "us-east-1"
    lake_s3_use_ssl: bool = False
    lake_access_key: str = Field(
        default="lakeadmin",
        validation_alias=AliasChoices("AIRFLOW_LAKE_ACCESS_KEY", "LAKE_ACCESS_KEY"),
    )
    lake_secret_key: str = Field(
        default="lakeadmin123",
        validation_alias=AliasChoices("AIRFLOW_LAKE_SECRET_KEY", "LAKE_SECRET_KEY"),
    )

    default_timezone: str = "Asia/Ho_Chi_Minh"
    default_retries: int = 2
    watermark_prefix: str = "_control/watermarks"

settings = Config()

@dataclass(frozen=True)
class WatermarkKey:
    pipeline: str
    source: str
    dataset: str

    def object_key(self) -> str:
        parts = (self.pipeline, self.source, self.dataset)
        if not all(part and part.replace("_", "").replace("-", "").isalnum() for part in parts):
            raise ValueError("Watermark names may contain only letters, digits, hyphens, and underscores")
        return f"{settings.watermark_prefix}/{self.pipeline}/{self.source}/{self.dataset}.json"
