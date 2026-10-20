from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="tiki_daily_crawl",
    description="Crawl Tiki thời trang (listing → detail → sellers → reviews) rồi nạp bronze/tiki",
    schedule="0 2 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["btl", "tầng-1", "crawl"],
)
def tiki_daily_crawl():

    # Các stage chạy tuần tự vì dùng chung giới hạn tốc độ ~1 req/s; chạy song song sẽ bị WAF chặn.
    # Bị chặn (Blocked) thì retry sau 1 giờ, crawler tiếp tục từ chỗ dở trong snapshot cùng ngày.
    def stage(name: str, stages: list[str], hours: int):
        @task(task_id=name, retries=4, retry_delay=timedelta(hours=1),
              execution_timeout=timedelta(hours=hours))
        def _run() -> None:
            from src.crawl import tiki

            tiki.run(stages)

        return _run()

    @task(execution_timeout=timedelta(hours=1))
    def bronze() -> None:
        from src.common.config import connect, new_batch_id
        from src.ingest.bronze import TIKI_KINDS, load_tiki

        con, batch_id = connect(), new_batch_id()
        for kind in TIKI_KINDS:
            load_tiki(con, kind, batch_id)

    # Listing ~600 request, đợt 20 request + nghỉ 15 phút ≈ 8 giờ; detail chạy trong lúc nghỉ nên chung một task.
    (stage("listing_detail", ["listing", "detail"], 16) >> stage("sellers", ["sellers"], 3)
     >> stage("reviews", ["reviews"], 6) >> bronze())


tiki_daily_crawl()
