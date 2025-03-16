from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 1,
}

dag = DAG(
    dag_id='bash_operator_example',
    default_args=default_args,
    catchup=False
)

# BashOperator를 사용한 Task 정의
task_1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, Airflow!"',
    dag=dag
)

task_2 = BashOperator(
    task_id='list_files',
    bash_command='ls -l',
    dag=dag
)

# Task 실행 순서 정의
task_1 >> task_2
