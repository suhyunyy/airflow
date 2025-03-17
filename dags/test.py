from airflow import DAG
from datetime import datetime, timedelta

dag = DAG(
    dag_id="example_schedule",
    start_date=datetime(2025, 3, 1),
    schedule_interval="0 6 * * *",  # 매일 오전 6시 실행
    catchup=False
)


