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
    # id: url_key (url_key chỉ để đặt tên thư mục khi chia file theo danh mục).
    crawl_tiki_roots: dict[int, str] = {915: "thoi-trang-nam", 1686: "giay-dep-nam", 27616: "tui-thoi-trang-nam"}
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

    # web riêng của các hãng: Haravan/Sapo/Shopify cùng mở /collections/all/products.json, không có WAF.
    # Haravan có inventory_quantity theo biến thể và sole_quantity (số đã bán); Sapo chỉ có tồn kho; Shopify
    # chỉ có available. "currency" mặc định VND; site nước ngoài khai báo USD.
    crawl_brands_user_agent: str = crawl_tiki_user_agent
    crawl_brands_min_interval: float = 1.0  # giây giữa 2 request tới CÙNG một site
    crawl_brands_workers: int = 6  # số site crawl song song
    crawl_brands_shopify_interval: float = 3.0  # các site Shopify chạy lần lượt, chung giới hạn theo IP
    crawl_brands_sites: dict[str, dict[str, str]] = {
        "aristino": {"domain": "aristino.com", "platform": "haravan"},
        "biluxury": {"domain": "biluxury.vn", "platform": "haravan"},
        "kenta": {"domain": "kenta.vn", "platform": "haravan"},
        "torano": {"domain": "torano.vn", "platform": "haravan"},
        "icondenim": {"domain": "icondenim.com", "platform": "haravan"},
        "pantio": {"domain": "pantio.vn", "platform": "haravan"},
        "nem": {"domain": "nemshop.vn", "platform": "haravan"},
        "evadeeva": {"domain": "evadeeva.com.vn", "platform": "haravan"},
        "marc": {"domain": "marc.com.vn", "platform": "haravan"},
        "chicland": {"domain": "chicland.vn", "platform": "haravan"},
        "coupletx": {"domain": "coupletx.com", "platform": "haravan"},
        "limeorange": {"domain": "limeorange.vn", "platform": "haravan"},
        "cocosin": {"domain": "cocosin.vn", "platform": "haravan"},
        "dottie": {"domain": "dottie.vn", "platform": "haravan"},
        "hades": {"domain": "hades.vn", "platform": "haravan"},
        "dirtycoins": {"domain": "dirtycoins.vn", "platform": "haravan"},
        "degrey": {"domain": "degrey.vn", "platform": "haravan"},
        "outerity": {"domain": "outerity.com", "platform": "haravan"},
        "swe": {"domain": "swe.vn", "platform": "haravan"},
        "dincox": {"domain": "dincox.com", "platform": "haravan"},
        "bitis": {"domain": "bitis.com.vn", "platform": "haravan"},
        "rabity": {"domain": "rabity.vn", "platform": "haravan"},
        "teelab": {"domain": "teelab.vn", "platform": "sapo"},
        "davies": {"domain": "davies.vn", "platform": "sapo"},
        "clownz": {"domain": "clownz.vn", "platform": "sapo"},
        "yame": {"domain": "yame.vn", "platform": "shopify"},
        "stussy": {"domain": "www.stussy.com", "platform": "shopify", "currency": "USD"},
        "gymshark": {"domain": "gymshark.com", "platform": "shopify", "currency": "USD"},
        "everlane": {"domain": "www.everlane.com", "platform": "shopify", "currency": "USD"},
        "allbirds": {"domain": "www.allbirds.com", "platform": "shopify", "currency": "USD"},
        "kith": {"domain": "kith.com", "platform": "shopify", "currency": "USD"},
        "taylorstitch": {"domain": "taylorstitch.com", "platform": "shopify", "currency": "USD"},
        "outerknown": {"domain": "outerknown.com", "platform": "shopify", "currency": "USD"},
        "cotopaxi": {"domain": "www.cotopaxi.com", "platform": "shopify", "currency": "USD"},
        "pangaia": {"domain": "pangaia.com", "platform": "shopify", "currency": "USD"},
        "aimeleondore": {"domain": "aimeleondore.com", "platform": "shopify", "currency": "USD"},
        "bonobos": {"domain": "www.bonobos.com", "platform": "shopify", "currency": "USD"},
        "tentree": {"domain": "tentree.com", "platform": "shopify", "currency": "USD"},
    }

    # API nội bộ của web Zara (không phải API công khai). robots.txt cấm /*/status/*?ajax=true (tồn kho) nên
    # không gọi; chỉ lấy danh mục và lưới sản phẩm theo danh mục.
    crawl_zara_user_agent: str = crawl_tiki_user_agent
    crawl_zara_store: str = "vn/vi"  # cửa hàng Việt Nam, giá VND
    crawl_zara_sections: list[str] = ["WOMAN", "MAN", "KID"]
    crawl_zara_min_interval: float = 2.0

    # API nội bộ của web Uniqlo Mỹ (store vn đòi client id). Tồn kho theo SKU bị chặn trần ở 11 (= "11+").
    crawl_uniqlo_user_agent: str = crawl_tiki_user_agent
    crawl_uniqlo_store: str = "us"
    crawl_uniqlo_lang: str = "en"
    crawl_uniqlo_genders: dict[int, str] = {22210: "women", 22211: "men", 22212: "kids", 22213: "baby"}
    crawl_uniqlo_min_interval: float = 1.0
    crawl_uniqlo_detail_refresh_days: int = 7
    crawl_uniqlo_review_pages: int = 5  # 20 review/trang

    def crawl_config(self, source: str) -> dict:
        prefix = f"crawl_{source}_"
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

    # pipeline multi-agent (src/): crawl → bronze → normalize → media → silver → gold
    lakehouse_dir: Path = PROJECT_ROOT / "data" / "lakehouse"  # bronze/normalized/silver/gold/media (parquet cục bộ)
    agents_dir: Path = PROJECT_ROOT / "data" / "_agents"  # log sự kiện, khoá, trạng thái từng lần chạy
    agents_workers: int = 8
    agents_task_retries: int = 1
    agents_crawl_timeout_hours: dict[str, float] = {"brands": 6, "zara": 4, "uniqlo": 10, "tiki": 20}
    # Ảnh/video: tải theo URL từ bảng media chuẩn hoá. Đĩa đang đầy nên có trần cho mỗi lần chạy.
    media_max_per_product: int = 4
    media_workers: int = 8
    media_host_concurrency: int = 4
    media_max_files_per_run: int = 50_000  # trần mỗi nguồn mỗi lần chạy
    media_max_bytes_per_run: int = 4 * 1024**3
    media_min_free_gb: int = 30  # dưới ngưỡng này thì dừng tải
    media_video: bool = True
    media_video_max_mb: int = 30
    media_zara_width: int = 1024  # URL ảnh Zara có tham số {width}
    # Tỷ giá cố định để so sánh giá giữa các nguồn trong gold (price_vnd); cập nhật khi cần.
    fx_to_vnd: dict[str, float] = {"VND": 1.0, "USD": 25_400.0, "BRL": 4_800.0}

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
