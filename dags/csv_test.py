from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

# CSV 파일 경로
CSV_FILE_PATH = "dags/data.csv"
OUTPUT_FILE_PATH = "dags/output.csv"

# 데이터 변환 함수
def transform_csv():
    # CSV 파일 읽기
    df = pd.read_csv(CSV_FILE_PATH)
    
    # 데이터 변환 (예: 컬럼명 변경 및 새로운 컬럼 추가)
    df.rename(columns={"old_column": "new_column"}, inplace=True)
    df["processed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 변환된 데이터 저장
    df.to_csv(OUTPUT_FILE_PATH, index=False)
    print("Data transformation complete. Saved to", OUTPUT_FILE_PATH)

# DAG 설정
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "csv_transform_dag",
    default_args=default_args,
    description="DAG to read and transform CSV file",
    schedule_interval="@daily",
    catchup=False,
)

# PythonOperator Task
transform_task = PythonOperator(
    task_id="transform_csv_task",
    python_callable=transform_csv,
    dag=dag,
)

# DAG 실행 순서
transform_task







# import datetime as dt
# from datetime import timedelta

# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator

# import pandas as pd

# def csvToJson():
#     df = pd.read_csv('dags/data.csv')
#     for i, r in df.iterrows():
#         print(r['name'])
#     df.to_json('dags/fromAirflow.json', orient='records')

# default_args = {

#     'start_date': dt.datetime(2020, 3, 18),
#     'retries': 1,
#     'retry_delay': dt.timedelta(minutes=5),
# }

# with DAG('MyCSVDAG',
#          default_args=default_args,
#          schedule_interval=timedelta(minutes=5),  # '0 * * * *',
#          ) as dag:
#     print_starting = BashOperator(task_id='starting',
#                                   bash_command='echo "I am reading the CSV now....."')

#     csvJson = PythonOperator(task_id='convertCSVtoJson',
#                              python_callable=csvToJson)

# print_starting >> csvJson