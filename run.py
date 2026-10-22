from __future__ import annotations

import argparse
import time

from src.common.config import get_logger

log = get_logger("run")


def step_download(only):
    from src.ingest import download
    download.run(only)


CRAWLERS = ["brands", "zara", "uniqlo", "tiki"]


def step_crawl(only):
    import importlib
    from concurrent.futures import ThreadPoolExecutor

    # Mỗi nguồn một host và một khoá riêng nên chạy song song; Tiki chậm nhất (nghỉ chờ WAF) không giữ chân nguồn khác.
    names = [c for c in CRAWLERS if not only or c in only]
    with ThreadPoolExecutor(len(names)) as ex:
        futs = {c: ex.submit(importlib.import_module(f"src.crawl.{c}").run) for c in names}
        for c, fu in futs.items():
            try:
                fu.result()
            except Exception:
                log.exception("[%s] crawl lỗi", c)


def step_bronze(only):
    from src.ingest import bronze
    bronze.run(only)


def step_silver(only):
    from src.silver import run as silver
    silver.run(only)


def _per_source(package: str, only) -> None:
    import importlib
    for src in ["olist", "hm"]:
        if not only or src in only:
            importlib.import_module(f"src.{package}.{src}").run()


def step_load(only):
    from src.warehouse import load
    load.run(only)


def step_gold(only):
    _per_source("gold", only)


def step_olap(only):
    _per_source("olap", only)


def step_mine(only):
    _per_source("mining", only)


STEPS = {"download": step_download, "crawl": step_crawl, "bronze": step_bronze, "silver": step_silver,
         "gold": step_gold, "load": step_load, "olap": step_olap, "mine": step_mine}


def main() -> None:
    ap = argparse.ArgumentParser(description="Pipeline BTL Data Lakehouse")
    ap.add_argument("step", choices=[*STEPS, "all"])
    ap.add_argument("--only", nargs="*", help="giới hạn nguồn: hm olist shopee tiki brands zara uniqlo")
    args = ap.parse_args()

    # crawl chạy hàng giờ và theo lịch riêng (DAG tiki_daily_crawl) nên không nằm trong `all`.
    for name in ([s for s in STEPS if s != "crawl"] if args.step == "all" else [args.step]):
        t0 = time.perf_counter()
        log.info("=== %s ===", name)
        STEPS[name](args.only)
        log.info("=== %s xong trong %.1fs ===", name, time.perf_counter() - t0)


if __name__ == "__main__":
    main()
