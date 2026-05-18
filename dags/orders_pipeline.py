from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mmds_engineer',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'orders_pipeline',
    default_args=default_args,
    schedule_interval='@once', # jalan sekali
    catchup=False,
    max_active_runs=1,
    description='Orders API -> Spark -> ClickHouse'
) as dag:

    ingest_stream = BashOperator(
        task_id='fetch_orders',
        bash_command='python /opt/airflow/dags/scripts/fetch_orders.py'
    )

    process_analytics = BashOperator(
        task_id='process_orders_spark',
        bash_command='python /opt/airflow/dags/scripts/process_orders_spark.py'
    )

    ingest_stream >> process_analytics