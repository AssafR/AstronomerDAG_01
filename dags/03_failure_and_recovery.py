from __future__ import annotations

import random
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

DEFAULT_ARGS = {"retries": 0}

with DAG(
    dag_id="03_failure_and_recovery",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "failure", "recovery"],
    params={
        "fail_prob": Param(0.5, type="number", description="Probability of failure in maybe_fail (0.0–1.0)"),
    },
) as dag:

    @task
    def start() -> int:
        return 10

    @task
    def maybe_fail(x: int, fail_prob: float) -> int:
        if random.random() < fail_prob:
            raise ValueError(f"Random failure (p={fail_prob})")
        return x * 2

    @task
    def end(x: int) -> None:
        print(f"Final value: {x}")

    end(maybe_fail(start(), dag.params["fail_prob"]))
