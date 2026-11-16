from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

ENV = {
    "DATA_PATH": "/opt/airflow/data",
    "BASE_URL": "https://d37ci6vzurychx.cloudfront.net/trip-data",
}

with DAG(
    dag_id="taxi_bronze_manual",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "year_month": Param(
            "2026-01",
            type="string",
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Mês a carregar, formato AAAA-MM",
        )
    },
    tags=["taxi", "bronze"],
) as dag:

    download_raw = BashOperator(
        task_id="download_raw",
        env=ENV,
        append_env=True,
        retries=2,
        retry_delay=timedelta(minutes=1),
        bash_command="""
        set -euo pipefail
        FILE="yellow_tripdata_{{ params.year_month }}.parquet"
        mkdir -p "$DATA_PATH/raw"
        curl -fL -o "$DATA_PATH/raw/$FILE.tmp" "$BASE_URL/$FILE"
        mv "$DATA_PATH/raw/$FILE.tmp" "$DATA_PATH/raw/$FILE"
        ls -lh "$DATA_PATH/raw/$FILE"
        """,
    )

    run_bronze = BashOperator(
        task_id="run_bronze",
        env=ENV,
        append_env=True,
        bash_command=(
            "python /opt/airflow/jobs/bronze.py "
            "--year-month {{ params.year_month }} "
            "--base-path $DATA_PATH"
        ),
    )

    download_raw >> run_bronze