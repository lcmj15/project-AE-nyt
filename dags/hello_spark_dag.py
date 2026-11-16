from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_spark",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["estudo"],
) as dag:

    run_spark = BashOperator(
        task_id="run_hello_spark",
        bash_command="python /opt/airflow/jobs/hello_spark.py",
    )