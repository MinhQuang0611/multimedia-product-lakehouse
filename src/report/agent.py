import duckdb

from src.common.core import REPORTS, S, Agent, Context, Task, layer, now_iso


def silver_glob(entity: str) -> str | None:
    d = layer("silver", entity)
    return (d / "*" / "*.parquet").as_posix() if any(d.glob("*/*.parquet")) else None


class ReportAgent(Agent):
    """Tổng hợp kết quả mọi agent + độ phủ dữ liệu từng nguồn thành báo cáo markdown."""

    name = "report"

    def run(self, task: Task, ctx: Context) -> dict:
        lines = [f"# Báo cáo pipeline `{ctx.run_id}`", "", f"Tạo lúc {now_iso()}", "",
                 "## Task", "", "| Task | Trạng thái | Thời gian | Kết quả |", "|---|---|---|---|"]
        for t in ctx.tasks.values():
            if t.id == task.id:
                continue
            res = ", ".join(f"{k}={v}" for k, v in t.result.items() if not isinstance(v, (dict, list)) and k != "log")
            lines.append(f"| {t.id} | {t.status}{' — ' + t.error if t.error else ''} | {t.started or ''} → {t.ended or ''} | {res[:160]} |")
        cov = self.coverage()
        if cov:
            lines += ["", "## Độ phủ dữ liệu theo nguồn (silver)", "",
                      "| Nguồn | Sản phẩm | Dòng snapshot | Biến thể | Media (ok/mục tiêu) | Review | Từ ngày | Đến ngày | % thiếu giá | % thiếu tên |",
                      "|---|---|---|---|---|---|---|---|---|---|"]
            lines += [f"| {r[0]} | {r[1]:,} | {r[2]:,} | {r[3]:,} | {r[4]:,}/{r[5]:,} | {r[6]:,} | {r[7]} | {r[8]} | {r[9]} | {r[10]} |"
                      for r in cov]
        lines += self.other_entities()
        errors = [t for t in ctx.tasks.values() if t.status in ("failed", "skipped")]
        if errors:
            lines += ["", "## Lỗi / bị bỏ qua", ""] + [f"- **{t.id}** ({t.status}): {t.error}" for t in errors]
        text = "\n".join(lines) + "\n"
        out = REPORTS / "pipeline"
        out.mkdir(parents=True, exist_ok=True)
        (out / f"run_{ctx.run_id}.md").write_text(text, encoding="utf-8")
        (out / "latest.md").write_text(text, encoding="utf-8")
        return {"report": str(out / f"run_{ctx.run_id}.md")}

    @staticmethod
    def other_entities() -> list[str]:
        """Số dòng silver của các entity ngoài sản phẩm, theo nguồn."""
        con = duckdb.connect()
        names = ["category", "customer", "order", "order_item", "payment", "location", "seller", "review"]
        res: dict[str, dict[str, int]] = {}
        for e in names:
            g = silver_glob(e)
            if g:
                for src, n in con.execute(f"SELECT source, count(*) FROM read_parquet('{g}', hive_partitioning=true, union_by_name=true) GROUP BY 1").fetchall():
                    res.setdefault(src, {})[e] = n
        if not res:
            return []
        out = ["", "## Các entity khác (silver, số dòng)", "", "| Nguồn | " + " | ".join(names) + " |", "|---|" + "---|" * len(names)]
        out += [f"| {s} | " + " | ".join(f"{res[s].get(e, 0):,}" for e in names) + " |" for s in sorted(res)]
        return out

    @staticmethod
    def coverage() -> list[tuple]:
        p = silver_glob("product")
        if not p:
            return []
        con = duckdb.connect()
        v, m, r = silver_glob("variant"), silver_glob("media"), silver_glob("review")
        con.execute(f"CREATE TEMP VIEW p AS SELECT * FROM read_parquet('{p}', hive_partitioning=true, union_by_name=true)")
        for name, g in (("v", v), ("m", m), ("r", r)):
            con.execute(f"CREATE TEMP VIEW {name} AS SELECT * FROM read_parquet('{g}', hive_partitioning=true, union_by_name=true)"
                        if g else f"CREATE TEMP VIEW {name} AS SELECT NULL::VARCHAR source, NULL::VARCHAR download_status WHERE false")
        return con.execute(f"""
            SELECT p.source, count(DISTINCT product_key), count(*),
                   coalesce(any_value((SELECT count(*) FROM v WHERE v.source = p.source)), 0),
                   coalesce(any_value((SELECT count(*) FROM m WHERE m.source = p.source AND download_status = 'ok' AND m."position" < {S.media_max_per_product})), 0),
                   coalesce(any_value((SELECT count(*) FROM m WHERE m.source = p.source AND m."position" < {S.media_max_per_product})), 0),
                   coalesce(any_value((SELECT count(*) FROM r WHERE r.source = p.source)), 0),
                   min(snapshot_date), max(snapshot_date),
                   round(100 * avg((price IS NULL)::INT), 1), round(100 * avg((name IS NULL)::INT), 1)
            FROM p GROUP BY p.source ORDER BY p.source""").fetchall()
