from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task


DEFAULT_ARGS = {
    "owner": "teaching",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="demo_green_to_red",
    start_date=datetime(2025, 1, 1),
    schedule=None,           # manual trigger in class
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "demo"],
) as dag:

    @task
    def extract() -> dict:
        # lightweight “fake data”
        return {"rows": 3, "source": "in-memory"}

    @task
    def transform(payload: dict) -> dict:
        # succeed in v1
        payload["rows_transformed"] = payload["rows"] * 10
        return payload

        # raise ValueError("Intentional teaching failure: transform() crashed")

    extract() >> transform()
