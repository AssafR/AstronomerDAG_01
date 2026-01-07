from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

DEFAULT_ARGS = {"retries": 0}

from include import business_logic

with DAG(
    dag_id="05_multi_file_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching", "multifile"],
    params={"fail": Param(False, type="boolean", description="Fail validation from business logic.")},
) as dag:
    """Multi-file example: DAG wiring here, logic in include/business_logic.py.

    Teaching goals:
    - DAG file stays short and declarative (mostly wiring).
    - Business logic is imported from a regular Python module.
    """

    @task(task_id="make_payload")
    def t_make_payload() -> dict:
        return business_logic.make_payload()

    @task(task_id="validate_payload")
    def t_validate_payload(payload: dict, fail: bool) -> dict:
        return business_logic.validate_payload(payload, fail)

    @task(task_id="summarize")
    def t_summarize(payload: dict) -> None:
        print(business_logic.summarize(payload))

    payload = t_make_payload()
    validated = t_validate_payload(payload, fail=dag.params["fail"])
    t_summarize(validated)
