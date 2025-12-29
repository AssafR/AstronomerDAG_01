from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

DEFAULT_ARGS = {"retries": 0}

with DAG(
    dag_id="demo_green_to_red",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["teaching"],
) as dag:

    @task
    def step_ok() -> dict:
        return {"value": 42}

    @task
    def step_maybe_fail(payload: dict, fail: bool = False) -> None:
        if fail:
            raise ValueError("Intentional teaching failure")
        print(f"All good. payload={payload}")

    step_maybe_fail(step_ok(), fail=False)
