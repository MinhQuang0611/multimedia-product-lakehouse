from __future__ import annotations

import logging
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import yaml

if TYPE_CHECKING:
    from src.common.storage import LakePath

ROOT = Path(__file__).resolve().parents[2]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


@cache
def load_config() -> dict:
    with open(ROOT / "configs" / "config.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class Paths:
    raw: Path
    lake: LakePath
    reports: Path

    @property
    def bronze(self) -> "LakePath":
        return self.lake / "bronze"

    @property
    def silver(self) -> "LakePath":
        return self.lake / "silver"

    @property
    def gold(self) -> "LakePath":
        return self.lake / "gold"

    @property
    def quarantine(self) -> "LakePath":
        return self.silver / "_quarantine"


def paths() -> Paths:
    from src.common.storage import LakePath

    p = load_config()["paths"]
    return Paths(raw=ROOT / p["raw"], lake=LakePath(), reports=ROOT / p["reports"])


def connect() -> duckdb.DuckDBPyConnection:
    cfg = load_config()["duckdb"]
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{os.environ.get('DUCKDB_MEMORY_LIMIT', cfg['memory_limit'])}'")
    con.execute(f"SET threads={int(cfg['threads'])}")
    con.execute("SET preserve_insertion_order=false")  # giảm RAM khi ghi file lớn
    tmp = ROOT / "data" / "_duckdb_tmp"
    tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{tmp.as_posix()}'")

    from src.common.storage import duckdb_s3_setup, lake_settings
    if lake_settings()["backend"] == "s3":
        duckdb_s3_setup(con)
    return con


def new_batch_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{uuid.uuid4().hex[:6]}"


def sql_path(p) -> str:
    return p.as_posix().replace("'", "''")
