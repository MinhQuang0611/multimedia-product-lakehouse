from __future__ import annotations

import argparse
import importlib
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

STAGES = ["listing", "detail", "images", "sellers", "reviews"]
CRAWLERS = ["brands", "zara", "uniqlo", "tiki"]
CRAWL_DIR = Path(__file__).resolve().parent / "airflow" / "scripts" / "crawl"
AIRFLOW_ROOT = CRAWL_DIR.parents[1]


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="Crawler sản phẩm thời trang")
    ap.add_argument("command", choices=["crawl"], nargs="?", default="crawl")
    ap.add_argument("--only", nargs="*", choices=CRAWLERS, help="Chỉ chạy các nguồn được chỉ định")
    ap.add_argument("--stages", nargs="*", choices=STAGES,
                    help="Chỉ chạy các stage Tiki được chỉ định")
    ap.add_argument("--limit", type=int, help="Giới hạn số mục mỗi stage (chạy thử)")
    ap.add_argument("--roots", nargs="*", type=int, help="Ghi đè các danh mục Tiki gốc")
    args = ap.parse_args()
    for p in (AIRFLOW_ROOT, CRAWL_DIR):
        if str(p) not in sys.path:
            sys.path.insert(0, str(p))
    from lib.config import get_logger

    log = get_logger("run")
    calls = {
        "brands": lambda m: m.run(),
        "zara": lambda m: m.run(args.limit),
        "uniqlo": lambda m: m.run(args.limit),
        "tiki": lambda m: m.run(args.stages, args.limit, args.roots),
    }
    # Mỗi nguồn một host và một khoá riêng nên chạy song song; Tiki chậm nhất (nghỉ chờ WAF) không giữ chân nguồn khác.
    names = args.only or CRAWLERS
    with ThreadPoolExecutor(len(names)) as ex:
        futs = {c: ex.submit(calls[c], importlib.import_module(c)) for c in names}
        for c, fu in futs.items():
            try:
                fu.result()
            except Exception:
                log.exception("[%s] crawl lỗi", c)


if __name__ == "__main__":
    main()
