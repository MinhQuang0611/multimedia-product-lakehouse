from __future__ import annotations

import argparse
import runpy
import sys
from pathlib import Path

STAGES = ["listing", "detail", "images", "sellers", "reviews"]
CRAWLER_SCRIPT = Path(__file__).resolve().parent / "airflow" / "scripts" / "crawl" / "tiki.py"
AIRFLOW_ROOT = CRAWLER_SCRIPT.parents[2]


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Tiki image crawler")
    ap.add_argument("command", choices=["crawl"], nargs="?", default="crawl")
    ap.add_argument("--stages", nargs="*", choices=STAGES,
                    help="Chỉ chạy các stage được chỉ định")
    ap.add_argument("--limit", type=int, help="Giới hạn số mục mỗi stage (chạy thử)")
    ap.add_argument("--roots", nargs="*", type=int, help="Ghi đè các danh mục Tiki gốc")
    args = ap.parse_args()
    if str(AIRFLOW_ROOT) not in sys.path:
        sys.path.insert(0, str(AIRFLOW_ROOT))
    crawler = runpy.run_path(str(CRAWLER_SCRIPT))
    crawler["run"](args.stages, args.limit, args.roots)


if __name__ == "__main__":
    main()
