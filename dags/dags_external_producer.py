from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

dag_a = DAG(
    dag_id="dag_a",
    start_date=datetime.now() - timedelta(seconds=1),  # 현재 시간 기준으로 실행
)

task_a = EmptyOperator(
    task_id="task_a",
    dag=dag_a
)