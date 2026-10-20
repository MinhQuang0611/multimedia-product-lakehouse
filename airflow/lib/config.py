import importlib.util
import logging
import sys
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.storage import LakePath

ROOT = Path(__file__).resolve().parents[2]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s", datefmt="%H:%M:%S")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


@cache
def airflow_settings():
    """Load project-local Airflow settings without shadowing Apache's package."""
    name = "project_airflow_config"
    if name in sys.modules:
        return sys.modules[name].settings
    spec = importlib.util.spec_from_file_location(name, ROOT / "airflow" / "config.py")
    if spec is None or spec.loader is None:
        raise RuntimeError("Cannot load airflow/config.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module.settings


@cache
def load_config() -> dict:
    s = airflow_settings()
    return {
        "paths": {"raw": str(s.raw_dir)},
        "lake": {
            "backend": s.lake_backend,
            "local_root": str(s.lake_local_root),
            "s3": {"endpoint": s.lake_s3_endpoint, "bucket": s.lake_s3_bucket,
                   "region": s.lake_s3_region, "use_ssl": s.lake_s3_use_ssl,
                   "access_key": s.lake_access_key, "secret_key": s.lake_secret_key},
        },
        "crawl": {"tiki": s.tiki_crawl_config()},
    }


@dataclass(frozen=True)
class Paths:
    raw: Path
    lake: LakePath


def paths() -> Paths:
    from lib.storage import LakePath
    return Paths(raw=ROOT / load_config()["paths"]["raw"], lake=LakePath())
