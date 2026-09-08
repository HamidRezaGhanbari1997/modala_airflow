from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from mahidasht.discovery import discover_service_ids
from mahidasht.services import fetch_and_load_service, load_providers_config

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def build_dag(provider: str, service_ids: list[int]) -> DAG:
    dag = DAG(
        dag_id=f"mahidasht_{provider}_ingestion",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        default_args=DEFAULT_ARGS,
        tags=["mahidasht", provider, "sql-server"],
    )

    for service_id in service_ids:
        PythonOperator(
            task_id=f"fetch_service_{service_id}",
            python_callable=fetch_and_load_service,
            op_kwargs={"provider": provider, "service_id": service_id},
            dag=dag,
        )

    return dag


for _provider in load_providers_config().get("providers", []):
    _service_ids = discover_service_ids(_provider)
    _dag = build_dag(_provider, _service_ids)
    globals()[_dag.dag_id] = _dag
