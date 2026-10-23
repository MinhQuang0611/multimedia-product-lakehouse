import argparse
import sys

from src.bronze.agent import BronzeAgent
from src.common.core import SOURCES, STAGES, S
from src.crawl.agent import CrawlAgent
from src.gold.agent import GoldAgent
from src.media.agent import MediaAgent
from src.normalize.agent import NormalizeAgent
from src.report.agent import ReportAgent
from src.silver.agent import SilverAgent
from src.orchestrator.supervisor import Supervisor, build_plan


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="run.py pipeline", description="Hệ thống multi-agent: crawl → bronze → chuẩn hoá → silver → gold")
    ap.add_argument("--stages", nargs="*", choices=STAGES, default=STAGES, help="chỉ chạy các stage này")
    ap.add_argument("--sources", nargs="*", choices=SOURCES, default=SOURCES, help="chỉ xử lý các nguồn này")
    ap.add_argument("--limit", type=int, help="giới hạn số mục crawl mỗi nguồn (chạy thử)")
    ap.add_argument("--plan", action="store_true", help="chỉ in đồ thị task, không chạy")
    a = ap.parse_args(argv)

    agents = {x.name: x for x in (CrawlAgent(), BronzeAgent(), NormalizeAgent(), MediaAgent(), SilverAgent(), GoldAgent(), ReportAgent())}
    tasks = build_plan(a.sources, a.stages, {"limit": a.limit})
    if a.plan:
        for t in tasks:
            print(f"{t.id:<22} agent={t.agent:<9} deps={t.deps}{' (soft)' if t.soft else ''}")
        return 0
    S.agents_dir.mkdir(parents=True, exist_ok=True)
    ctx = Supervisor(agents).run_plan(tasks, a.sources, {"stages": a.stages, "limit": a.limit})
    if ctx.opts.get("skipped_locked"):
        return 0
    bad = [t for t in tasks if t.status == "failed" and not t.soft]
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
