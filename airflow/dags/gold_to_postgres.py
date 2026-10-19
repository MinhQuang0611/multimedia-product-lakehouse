from __future__ import annotations

from datetime import datetime

from airflow.sdk import dag, get_current_context, task

SOURCE = "hm"


@dag(
    dag_id="gold_minio_to_postgres",
    description="Gold (s3://lake/gold/hm) → staging gold_hm → star schema hm, kèm đối chiếu số dòng/doanh thu",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["btl", "tầng-2", "warehouse"],
)
def gold_minio_to_postgres():

    @task
    def list_gold_tables() -> list[str]:
        from src.warehouse.load import gold_tables

        tables = gold_tables(SOURCE)
        if not tables:
            raise ValueError(f"Không có bảng nào ở gold/{SOURCE} — chạy `python run.py gold` trước")
        return tables

    @task
    def prepare_postgres() -> None:
        from src.warehouse.load import prepare

        prepare(SOURCE)

    # Tối đa 2 bảng song song để không dồn RAM/IO.
    @task(max_active_tis_per_dagrun=2, retries=2)
    def load_table(table: str) -> dict:
        from src.warehouse.load import load_table as load

        return load(SOURCE, table, run_id=get_current_context()["run_id"])

    @task
    def reconcile(results: list[dict]) -> list[dict]:
        from src.warehouse.load import reconcile as check

        return check(SOURCE, [r["table"] for r in results])

    @task
    def build_star_schema() -> list[dict]:
        from src.warehouse.load import build

        return build(SOURCE, run_id=get_current_context()["run_id"])

    tables = list_gold_tables()
    loaded = load_table.expand(table=tables)
    prepare_postgres() >> loaded
    reconcile(loaded) >> build_star_schema()


gold_minio_to_postgres()
