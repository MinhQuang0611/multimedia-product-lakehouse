from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone

from src.common.config import get_logger, paths
from src.common.lake import scan, write_table

log = get_logger("silver.quality")


@dataclass(frozen=True)
class Rule:
    code: str
    violated_if: str  # biểu thức SQL trên các cột đã ép kiểu


def split_valid(con, staged_sql: str, rules: list[Rule], table: str,
                partition_by: list[str] | None = None, manifest: dict | None = None) -> dict:
    if rules:
        flags = ", ".join(f"CASE WHEN coalesce(({r.violated_if}), true) THEN '{r.code}' END"
                          for r in rules)
        errors_expr = f"list_filter([{flags}], x -> x IS NOT NULL)"
    else:
        errors_expr = "[]::VARCHAR[]"
    con.execute(f"CREATE OR REPLACE TEMP TABLE _checked AS "
                f"SELECT *, {errors_expr} AS _errors FROM ({staged_sql})")

    n_ok = write_table(con, "SELECT * EXCLUDE (_errors) FROM _checked WHERE len(_errors) = 0",
                       paths().silver / table, partition_by, manifest)
    n_bad = write_table(con, "SELECT * FROM _checked WHERE len(_errors) > 0",
                        paths().quarantine / table)

    by_code = dict(con.execute(
        "SELECT e, count(*) FROM (SELECT unnest(_errors) AS e FROM _checked) GROUP BY e ORDER BY e"
    ).fetchall())
    con.execute("DROP TABLE _checked")

    log.info("  silver/%-22s %12s hợp lệ | %9s quarantine %s",
             table, f"{n_ok:,}", f"{n_bad:,}", by_code or "")
    _append_quality_log(table, n_ok, n_bad, by_code)
    return {"table": table, "valid": n_ok, "quarantined": n_bad, "errors": by_code}


def _append_quality_log(table: str, n_ok: int, n_bad: int, by_code: dict) -> None:
    out = paths().reports / "quality_log.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    new = not out.exists()
    with open(out, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if new:
            w.writerow(["run_ts", "table", "valid", "quarantined", "errors"])
        w.writerow([datetime.now(timezone.utc).isoformat(timespec="seconds"), table, n_ok, n_bad,
                    ";".join(f"{k}={v}" for k, v in by_code.items())])


def missing_report(con, layer_path, label: str) -> list[tuple]:
    rel = scan(layer_path)
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {rel}").fetchall()
            if not r[0].startswith("_") and r[0] not in ("year", "month")]
    exprs = ", ".join(
        f"avg(CASE WHEN \"{c}\" IS NULL OR trim(CAST(\"{c}\" AS VARCHAR)) = '' THEN 1 ELSE 0 END)"
        for c in cols)
    total, *rates = con.execute(f"SELECT count(*), {exprs} FROM {rel}").fetchone()
    return [(label, c, total, round(100 * r, 3)) for c, r in zip(cols, rates)]
