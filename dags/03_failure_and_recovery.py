import random

from __future__ import annotations

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
        # In Astronomer: Trigger DAG -> Config -> set fail=true to force a failure
        "fail": Param(True, type="boolean", description="If true, the middle task fails intentionally."),
    },
) as dag:
    """Intentional failure + recovery demo.

    Teaching goals:
    - A DAG run can have mixed task states (green/red).
    - You can re-run from a specific node by clearing only the failed task(s).
    - Upstream tasks can stay successful; downstream tasks can be re-scheduled.
    """

    @task(task_id="start")
    def start() -> int:
        return 10



    @task(task_id="maybe_fail")
    def maybe_fail(x: int) -> int:
        if random.random() < 0.5:
            raise ValueError("Random failure (≈50%)")
        return x * 2


    @task(task_id="end")
    def end(x: int) -> None:
        print(f"Final value: {x}")

    x = start()
    y = maybe_fail(x)
    end(y)