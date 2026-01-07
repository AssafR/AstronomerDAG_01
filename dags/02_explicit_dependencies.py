from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

DEFAULT_ARGS = {"retries": 0}

with DAG(
    dag_id="02_explicit_dependencies",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "explicit", "operators"],
) as dag:
    """Explicit dependencies using >> / <<.

    Teaching goals:
    - Dependencies can be declared even when no data is passed between tasks.
    - This style is often clearer for branching/fan-in patterns.
    """

    @task(task_id="step_a")
    def step_a() -> None:
        print("A")

    @task(task_id="step_b")
    def step_b() -> None:
        print("B")

    @task(task_id="step_c")
    def step_c() -> None:
        print("C")

    a = step_a()
    b = step_b()
    c = step_c()

    # --- Explicit edges (control flow) ---
    a >> b >> c
