"""Chicago Taxi Trips: bronze (SODA → MinIO) → silver (Spark cleaning + quality gate) → gold (Spark KPIs).

Runs once by itself after `docker compose up` (schedule @once, DAGs unpaused at creation); trigger it again from the
UI or `airflow dags trigger taxi_pipeline` (param `force_ingest` re-downloads the months).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = "spark-submit --master local[*] --driver-memory ${SPARK_DRIVER_MEMORY:-2g} --jars ${SPARK_EXTRA_JARS}"


@dag(
    dag_id="taxi_pipeline",
    description="Chicago Taxi Trips medallion pipeline (bronze → silver → gold)",
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=1)},
    params={"force_ingest": Param(False, type="boolean", description="Re-download months already in bronze")},
    tags=["taxi", "medallion", "spark"],
)
def taxi_pipeline():
    @task
    def plan_months() -> list[dict]:
        """One ingestion task per month of [TRIPS_START, TRIPS_END)."""
        from pipeline.config import Settings
        from pipeline.ingest.soda import month_windows

        s = Settings.from_env()
        return [{"start": a.isoformat(), "end": b.isoformat()} for a, b in month_windows(s.trips_start, s.trips_end)]

    @task(max_active_tis_per_dag=3, execution_timeout=timedelta(hours=2))
    def ingest_month(window: dict, params: dict | None = None) -> dict:
        """Bronze: one month of raw CSV pages from the SODA API (skipped when already complete)."""
        from datetime import date

        from pipeline.config import Settings
        from pipeline.ingest.soda import ingest_month as ingest

        force = bool((params or {}).get("force_ingest"))
        return ingest(Settings.from_env(), date.fromisoformat(window["start"]), date.fromisoformat(window["end"]), force)

    @task
    def ingest_reference() -> None:
        """Bronze: the community-area reference (number → name)."""
        from pipeline.config import Settings
        from pipeline.ingest.soda import ingest_reference as ingest

        ingest(Settings.from_env())

    @task
    def check_bronze(manifests: list[dict]) -> int:
        """Fails early if a month came back empty (an API or filter problem, not a quiet month)."""
        empty = [m["window"] for m in manifests if not m["rows"]]
        if empty:
            raise ValueError(f"no rows ingested for {empty}")
        return sum(m["rows"] for m in manifests)

    silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/pipeline/spark/bronze_to_silver.py",
        execution_timeout=timedelta(hours=1),
    )
    gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{SPARK_SUBMIT} /opt/airflow/pipeline/spark/silver_to_gold.py",
        execution_timeout=timedelta(hours=1),
    )

    manifests = ingest_month.expand(window=plan_months())
    [check_bronze(manifests), ingest_reference()] >> silver >> gold


taxi_pipeline()
