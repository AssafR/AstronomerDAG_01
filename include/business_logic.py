"""Pure-Python business logic.

Teaching goals:
- Keep orchestration (Airflow) separate from business code.
- This module can be unit-tested without Airflow.
"""

def make_payload() -> dict:
    return {"value": 42}

def validate_payload(payload: dict, fail: bool) -> dict:
    if fail:
        raise ValueError("Intentional failure from business logic")
    # Add "realistic" validation work here.
    return payload

def summarize(payload: dict) -> str:
    return f"value={payload['value']}"
