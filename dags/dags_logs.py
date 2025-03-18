from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import random
import sqlite3  # 간단한 테스트용 DB
from datetime import datetime

# Task 1: CSV 데이터 읽기
def download_data():
    df = pd.read_csv("dags/input_data.csv")  # Airflow 환경에서 접근 가능하도록 절대 경로 사용
    print("데이터 로드 완료")
    return df.to_dict()  # XCom을 통해 데이터 전달

# Task 2: 데이터 처리 (old_column 삭제, age_group 추가)
def process_data(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(task_ids="download_data")  # XCom을 통해 데이터 가져오기
    df = pd.DataFrame(data_dict)  # Dictionary를 DataFrame으로 변환

    df = df.drop(columns=["old_column"])  # 필요 없는 컬럼 삭제
    df["age_group"] = df["age"].apply(lambda x: "Young" if x < 30 else "Adult")
    print("데이터 처리 완료")

    ti.xcom_push(key="processed_data", value=df.to_dict())  # XCom에 데이터 저장

# Task 3: 데이터 저장 (50% 확률로 실패)
def store_data(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(task_ids="process_data", key="processed_data")  # XCom에서 데이터 가져오기
    df = pd.DataFrame(data_dict)

    if random.choice([True, False]):
        raise ConnectionError("데이터베이스 연결 실패!")

    conn = sqlite3.connect("/opt/airflow/dags/test_db.sqlite")  # Airflow 환경에서 접근 가능하도록 경로 설정
    df.to_sql("users", conn, if_exists="replace", index=False)
    conn.close()
    print("데이터 저장 완료")

# DAG 정의
dag = DAG(
    "dag_csv_processing",
    schedule_interval="@daily",
    start_date=datetime(2025, 3, 1),
    catchup=False
)

# Task 정의
task_1 = PythonOperator(
    task_id="download_data",
    python_callable=download_data,
    dag=dag
)

task_2 = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

task_3 = PythonOperator(
    task_id="store_data",
    python_callable=store_data,
    provide_context=True,
    dag=dag
)

# 실행 순서 정의
task_1 >> task_2 >> task_3
