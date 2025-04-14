from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

with DAG(
    dag_id='backfill_example',
    start_date=pendulum.datetime(2025, 4, 10, tz="Asia/Seoul"),
    schedule_interval='@daily',
    catchup=True,
) as dag:
    EmptyOperator(task_id='run_this')