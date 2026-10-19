from __future__ import annotations

import os
from datetime import datetime, timezone

from src.common.config import ROOT, connect, get_logger, load_config, paths
from src.common.lake import scan

log = get_logger("warehouse.load")

def dw_settings() -> dict:
    from src.common.storage import load_dotenv

    load_dotenv(ROOT / "data_warehouse" / ".env")
    cfg = dict(load_config()["warehouse"])
    for k in ["host", "port", "dbname", "user", "password"]:
        cfg[k] = os.environ.get(f"DW_{k.upper()}", cfg[k])
    return cfg


def attach(con, read_only: bool = False) -> None:
    w = dw_settings()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH 'host={w['host']} port={w['port']} dbname={w['dbname']} "
                f"user={w['user']} password={w['password']}' AS pg "
                f"(TYPE postgres{', READ_ONLY' if read_only else ''})")


def dw_views(con, schema: str, tables: list[str]) -> None:
    attach(con, read_only=True)
    for t in tables:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {t} AS SELECT * FROM pg.{schema}.{t}")


def _pg(con, sql: str) -> None:
    con.execute("CALL postgres_execute('pg', ?)", [sql])


def gold_tables(source: str = "hm") -> list[str]:
    return [n for n in (paths().gold / source).children() if not n.startswith(".")]


def prepare(source: str = "hm") -> None:
    con = connect()
    attach(con)
    schema = f"gold_{source}"
    lake = set(gold_tables(source))
    stale = [t for (t,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = 'pg' AND schema_name = ?",
        [schema]).fetchall() if t not in lake]
    for t in stale:
        _pg(con, f'DROP TABLE IF EXISTS {schema}."{t}" CASCADE')
        log.info("  xoá staging cũ %s.%s", schema, t)


def load_table(source: str, table: str, run_id: str | None = None) -> dict:
    con = connect()
    attach(con)
    src, schema = paths().gold / source / table, f"gold_{source}"
    started = datetime.now(timezone.utc)

    _pg(con, f'DROP TABLE IF EXISTS {schema}."{table}__load"')
    con.execute(f'CREATE TABLE pg.{schema}."{table}__load" AS SELECT * FROM {scan(src)}')
    n = con.execute(f'SELECT count(*) FROM pg.{schema}."{table}__load"').fetchone()[0]

    finished = datetime.now(timezone.utc)
    _pg(con, f"""
        BEGIN;
        DROP TABLE IF EXISTS {schema}."{table}";
        ALTER TABLE {schema}."{table}__load" RENAME TO "{table}";
        COMMENT ON TABLE {schema}."{table}" IS 'Sao từ {src.as_posix()} lúc {finished:%Y-%m-%d %H:%M:%S}Z';
        INSERT INTO etl_audit.load_log (run_id, source_uri, target, rows, started_at, finished_at)
        VALUES ({_lit(run_id)}, '{src.as_posix()}', '{schema}.{table}', {n},
                '{started.isoformat()}', '{finished.isoformat()}');
        COMMIT;
    """)
    secs = (finished - started).total_seconds()
    log.info("  %-40s → %-32s %12s dòng  %6.1fs", src.as_posix(), f"{schema}.{table}", f"{n:,}", secs)
    return {"table": table, "rows": n, "seconds": round(secs, 1)}


def _lit(v: str | None) -> str:
    return "NULL" if v is None else "'" + v.replace("'", "''") + "'"


def reconcile(source: str, tables: list[str]) -> list[dict]:
    con = connect()
    attach(con)
    schema, out = f"gold_{source}", []
    for t in tables:
        src = scan(paths().gold / source / t)
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()}
        agg = "count(*), " + ("sum(revenue)" if "revenue" in cols else "NULL")
        lake = con.execute(f"SELECT {agg} FROM {src}").fetchone()
        dw = con.execute(f'SELECT {agg} FROM pg.{schema}."{t}"').fetchone()
        ok = lake[0] == dw[0] and (lake[1] is None or abs(lake[1] - dw[1]) < 1e-6 * max(1.0, abs(lake[1])))
        out.append({"table": t, "lake_rows": lake[0], "dw_rows": dw[0], "lake_revenue": lake[1],
                    "dw_revenue": dw[1], "ok": ok})
        log.info("  đối chiếu %-28s lake %12s | dw %12s %s", t, f"{lake[0]:,}", f"{dw[0]:,}",
                 "KHỚP" if ok else "LỆCH")
    bad = [r["table"] for r in out if not r["ok"]]
    if bad:
        raise AssertionError(f"Lake ↔ Postgres không khớp: {bad}")
    return out


def build(source: str = "hm", run_id: str | None = None) -> list[dict]:
    con = connect()
    attach(con)
    t0 = datetime.now(timezone.utc)
    _pg(con, f"CALL {source}.build({_lit(run_id)})")
    rows = con.execute("""
        SELECT target, rows, extract(epoch FROM finished_at - started_at) AS secs
        FROM pg.etl_audit.load_log WHERE started_at >= ? AND target LIKE ? ORDER BY id
    """, [t0, f"{source}.%"]).fetchall()
    for target, n, secs in rows:
        log.info("  %-36s %12s dòng  %6.1fs", target, f"{n:,}", secs)
    return [{"target": t, "rows": n, "seconds": round(s, 1)} for t, n, s in rows]


def run(only: list[str] | None = None) -> None:
    for source in (only or ["hm"]):
        tables = gold_tables(source)
        log.info("Gold %s → Postgres: %s", source, ", ".join(tables))
        prepare(source)
        for t in tables:
            load_table(source, t)
        reconcile(source, tables)
        log.info("Dựng star schema %s trong DW", source)
        build(source)
