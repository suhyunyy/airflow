from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import requests
import random
from datetime import datetime

# Task 1: 데이터 다운로드
def download_data():
    response = requests.get("https://people.sc.fsu.edu/~jburkardt/data/csv/airtravel.csv")
    open("/tmp/airtravel.csv", "w").write(response.text)

# Task 2: 데이터 처리
def process_data():
    df = pd.read_csv("/tmp/airtravel.csv")
    df["Processed"] = True
    df.to_csv("/tmp/processed_airtravel.csv", index=False)

# Task 3: 데이터 저장 (50% 확률로 실패)
def store_data():
    if not random.choice([True, False]):
        raise ConnectionError("데이터베이스 연결 실패!")

# DAG 정의
dag = DAG(
    "dag_simple_failure_example",
    schedule="@daily",
    start_date=datetime(2025, 3, 1),
    catchup=False
)

# Task 정의
task_1 = PythonOperator(task_id="download_data", python_callable=download_data, dag=dag)
task_2 = PythonOperator(task_id="process_data", python_callable=process_data, dag=dag)
task_3 = PythonOperator(task_id="store_data", python_callable=store_data, dag=dag)

# 실행 순서 정의
task_1 >> task_2 >> task_3