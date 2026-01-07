from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

DEFAULT_ARGS = {"retries": 0}

with DAG(
    dag_id="04_branching_fanout",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "fanout", "fanin"],
) as dag:
    """Fan-out / fan-in with TaskFlow.

    Teaching goals:
    - One upstream task feeds multiple parallel downstream tasks (fan-out).
    - Join results in a final task (fan-in).
    - In Astronomer Graph view, students can *see* parallel branches.
    """

    @task(task_id="start")
    def start() -> int:
        return 5

    @task(task_id="left_branch")
    def left_branch(x: int) -> int:
        return x + 1

    @task(task_id="right_branch")
    def right_branch(x: int) -> int:
        return x - 1

    @task(task_id="join")
    def join(values: list[int]) -> None:
        print(f"Joined values: {values}")

    s = start()

    # Fan-out
    l = left_branch(s)
    r = right_branch(s)

    # Fan-in
    join([l, r])
