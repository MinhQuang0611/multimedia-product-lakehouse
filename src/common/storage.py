from __future__ import annotations

import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from functools import cache
from pathlib import Path, PurePosixPath

from src.common.config import ROOT, load_config

COPY_WORKERS = 16


def load_dotenv(f: Path = ROOT / ".env") -> None:
    if f.exists():
        for line in f.read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


@cache
def lake_settings() -> dict:
    load_dotenv()
    cfg = dict(load_config()["lake"])
    cfg["backend"] = os.environ.get("LAKE_BACKEND", cfg["backend"])
    s3 = dict(cfg["s3"])
    s3["endpoint"] = os.environ.get("LAKE_S3_ENDPOINT", s3["endpoint"])
    s3["access_key"] = os.environ.get("LAKE_ACCESS_KEY", s3["access_key"])
    s3["secret_key"] = os.environ.get("LAKE_SECRET_KEY", s3["secret_key"])
    cfg["s3"] = s3
    return cfg


@cache
def s3_client():
    import boto3
    from botocore.config import Config

    s3 = lake_settings()["s3"]
    scheme = "https" if s3["use_ssl"] else "http"
    return boto3.client("s3", endpoint_url=f"{scheme}://{s3['endpoint']}",
                        aws_access_key_id=s3["access_key"], aws_secret_access_key=s3["secret_key"],
                        region_name=s3["region"], config=Config(max_pool_connections=COPY_WORKERS))


class LakePath:

    def __init__(self, key: str = ""):
        self.key = str(PurePosixPath(key)) if key else ""

    def __truediv__(self, part: str) -> LakePath:
        return LakePath(f"{self.key}/{part}" if self.key else part)

    def __repr__(self) -> str:
        return f"LakePath({self.as_posix()!r})"

    @property
    def name(self) -> str:
        return PurePosixPath(self.key).name

    @property
    def parent(self) -> LakePath:
        p = str(PurePosixPath(self.key).parent)
        return LakePath("" if p == "." else p)

    def with_name(self, name: str) -> LakePath:
        return self.parent / name

    @property
    def is_s3(self) -> bool:
        return lake_settings()["backend"] == "s3"

    @property
    def _local(self) -> Path:
        return ROOT / lake_settings()["local_root"] / self.key

    @property
    def _bucket(self) -> str:
        return lake_settings()["s3"]["bucket"]

    def as_posix(self) -> str:
        return f"s3://{self._bucket}/{self.key}" if self.is_s3 else self._local.as_posix()

    def _keys(self) -> list[str]:
        out, pag = [], s3_client().get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=self._bucket, Prefix=self.key.rstrip("/") + "/"):
            out += [o["Key"] for o in page.get("Contents", [])]
        return out

    def exists(self) -> bool:
        if not self.is_s3:
            return self._local.exists()
        if self._keys_exact():
            return True
        r = s3_client().list_objects_v2(Bucket=self._bucket, Prefix=self.key.rstrip("/") + "/", MaxKeys=1)
        return r.get("KeyCount", 0) > 0

    def _keys_exact(self) -> bool:
        try:
            s3_client().head_object(Bucket=self._bucket, Key=self.key)
            return True
        except Exception:
            return False

    def children(self) -> list[str]:
        if not self.is_s3:
            return sorted(p.name for p in self._local.iterdir() if p.is_dir()) if self._local.exists() else []
        prefix, names = self.key.rstrip("/") + "/", []
        for page in s3_client().get_paginator("list_objects_v2").paginate(
                Bucket=self._bucket, Prefix=prefix, Delimiter="/"):
            names += [c["Prefix"][len(prefix):].rstrip("/") for c in page.get("CommonPrefixes", [])]
        return sorted(names)

    def has_parquet(self) -> bool:
        if not self.is_s3:
            return self._local.exists() and any(self._local.rglob("*.parquet"))
        return any(k.endswith(".parquet") for k in self._keys())

    def read_text(self, encoding: str = "utf-8") -> str:
        if not self.is_s3:
            return self._local.read_text(encoding=encoding)
        return s3_client().get_object(Bucket=self._bucket, Key=self.key)["Body"].read().decode(encoding)

    def write_text(self, text: str, encoding: str = "utf-8") -> None:
        self.write_bytes(text.encode(encoding))

    def write_bytes(self, data: bytes, content_type: str | None = None) -> None:
        if not self.is_s3:
            self._local.parent.mkdir(parents=True, exist_ok=True)
            self._local.write_bytes(data)
            return
        args = {"Bucket": self._bucket, "Key": self.key, "Body": data}
        if content_type:
            args["ContentType"] = content_type
        s3_client().put_object(**args)

    def mkdir(self) -> None:
        if not self.is_s3:
            self._local.mkdir(parents=True, exist_ok=True)

    def remove(self) -> None:
        if not self.is_s3:
            if self._local.exists():
                shutil.rmtree(self._local)
            return
        keys = self._keys()
        for i in range(0, len(keys), 1000):
            s3_client().delete_objects(Bucket=self._bucket,
                                       Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})

    def replace(self, final: LakePath, attempts: int = 5) -> None:
        if not self.is_s3:
            for i in range(attempts):
                try:
                    if final._local.exists():
                        shutil.rmtree(final._local)
                    self._local.rename(final._local)
                    return
                except PermissionError:
                    if i == attempts - 1:
                        raise
                    time.sleep(0.5 * (i + 1))
            return
        final.remove()
        src_prefix, dst_prefix = self.key.rstrip("/") + "/", final.key.rstrip("/") + "/"
        cli, bucket = s3_client(), self._bucket

        def _copy(k: str) -> None:
            cli.copy_object(Bucket=bucket, Key=dst_prefix + k[len(src_prefix):],
                            CopySource={"Bucket": bucket, "Key": k})

        with ThreadPoolExecutor(COPY_WORKERS) as ex:
            list(ex.map(_copy, self._keys()))
        self.remove()


def duckdb_s3_setup(con) -> None:
    s3 = lake_settings()["s3"]
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        CREATE OR REPLACE SECRET lake_s3 (
            TYPE s3, KEY_ID '{s3['access_key']}', SECRET '{s3['secret_key']}',
            ENDPOINT '{s3['endpoint']}', REGION '{s3['region']}',
            URL_STYLE 'path', USE_SSL {str(bool(s3['use_ssl'])).lower()})
    """)
