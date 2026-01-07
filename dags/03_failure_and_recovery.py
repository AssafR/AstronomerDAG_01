from __future__ import annotations

import random
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

DEFAULT_ARGS = {"retries": 0}

with DAG(
    dag_id="03_failure_and_recovery",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "failure", "recovery"],
) as dag:

    @task
    def start() -> int:
        return 10

    @task
    def maybe_fail(x: int) -> int:
        if random.random() < 0.5:
            raise ValueError("Random failure (≈50%)")
        return x * 2

    @task
    def end(x: int) -> None:
        print(f"Final value: {x}")

    end(maybe_fail(start()))
