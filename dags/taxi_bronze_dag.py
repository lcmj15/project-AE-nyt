from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.bash import BashSensor

DATA_PATH = "/opt/airflow/data"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Mês da janela da execução, ex.: "2026-03"
YM = "{{ data_interval_start.strftime('%Y-%m') }}"
FILE = "yellow_tripdata_" + YM + ".parquet"

ENV = {"DATA_PATH": DATA_PATH, "BASE_URL": BASE_URL}

with DAG(
    dag_id="taxi_monthly",
    start_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["taxi", "bronze"],
) as dag:

    wait_for_file = BashSensor(
        task_id="wait_for_file",
        bash_command="curl -fsIL " + BASE_URL + "/" + FILE + " > /dev/null",
        mode="reschedule",
        poke_interval=60 * 60 * 24,      # checa 1x por dia (em segundos)
        timeout=60 * 60 * 24 * 90,       # desiste após 90 dias
        retries=0,
    )

    download_raw = BashOperator(
        task_id="download_raw",
        env=ENV,
        append_env=True,
        retries=2,
        retry_delay=timedelta(minutes=5),
        bash_command="""
        set -euo pipefail
        FILE="yellow_tripdata_""" + YM + """.parquet"
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
        bash_command="python /opt/airflow/jobs/bronze.py --year-month " + YM + " --base-path $DATA_PATH",
    )

    wait_for_file >> download_raw >> run_bronze