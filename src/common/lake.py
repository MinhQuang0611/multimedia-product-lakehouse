from __future__ import annotations

import json

from src.common.config import sql_path
from src.common.storage import LakePath


def scan(path: LakePath) -> str:
    return f"read_parquet('{sql_path(path)}/**/*.parquet', hive_partitioning=true, union_by_name=true)"


def write_table(con, select_sql: str, final: LakePath, partition_by: list[str] | None = None,
                manifest: dict | None = None) -> int:
    tmp = final.with_name(f".{final.name}.tmp")
    tmp.remove()
    tmp.parent.mkdir()

    # Không đọc lại bản tạm: trên Windows DuckDB giữ handle file, bước đổi tên sẽ bị từ chối.
    if partition_by:
        n = con.execute(f"""COPY ({select_sql}) TO '{sql_path(tmp)}'
                            (FORMAT parquet, COMPRESSION zstd, PARTITION_BY ({', '.join(partition_by)}))""").fetchone()[0]
    else:
        tmp.mkdir()
        n = con.execute(f"COPY ({select_sql}) TO '{sql_path(tmp / 'part-0.parquet')}' "
                        f"(FORMAT parquet, COMPRESSION zstd)").fetchone()[0]

    tmp.replace(final)
    if manifest is not None:
        (final / "_manifest.json").write_text(json.dumps({**manifest, "rows": n}, indent=2, default=str))
    return n
