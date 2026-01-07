from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

DEFAULT_ARGS = {"retries": 0}

# DAG is a class that represents a DAG (Directed Acyclic Graph)
# dag_id is the name of the DAG
# start_date is the date and time the DAG should run
# schedule is the schedule of the DAG
# catchup is whether to catch up on missed runs
# default_args are the default arguments for the DAG
# tags are the tags for the DAG
with DAG(
    dag_id="01_linear_taskflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "taskflow", "implicit"],
) as dag:
    """A minimal linear pipeline using the TaskFlow API.

    Teaching goals:
    - DAG files build a graph; they don't run tasks during parsing.
    - Calling a @task function creates a task node and returns an XComArg
      (a placeholder for future output), not the real data.
    - Passing an upstream XComArg into a downstream task creates an
      implicit dependency edge in the DAG.
    """

    @task(task_id="extract")
    def extract() -> dict:
        # In real projects, this might read from an API / DB / file.
        return {"value": 42}

    @task(task_id="transform")
    def transform(payload: dict) -> int:
        # Pretend we do "real" work here.
        return payload["value"] * 2

    @task(task_id="load")
    def load(result: int) -> None:
        print(f"Loaded result={result}")

    # --- "WIRING" (dependencies inferred from data flow) ---
    payload = extract()
    result = transform(payload)
    load(result)
