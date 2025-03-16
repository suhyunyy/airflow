import requests
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
import pendulum

# API 응답값을 기반으로 실행할 Task 선택
def check_weather():
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast?latitude=37.5665&longitude=126.9780&current_weather=true"
    )
    data = response.json()
    temperature = data["current_weather"]["temperature"]  # 현재 기온 가져오기

    if temperature >= 15:  # 15°C 이상이면 더운 날씨로 판단
        return "task_hot"
    else:
        return "task_cold"

# DAG 정의
with DAG(
    dag_id="branch_operator_weather_api_example",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # API 상태 확인 후 실행할 Task 선택
    branch_task = BranchPythonOperator(
        task_id="branching_weather_api",
        python_callable=check_weather
    )

    # 15°C 이상이면 실행될 Task
    task_hot = BashOperator(
        task_id="task_hot",
        bash_command="echo '오늘 날씨: 덥습니다!'"
    )

    # 15°C 미만이면 실행될 Task
    task_cold = BashOperator(
        task_id="task_cold",
        bash_command="echo '오늘 날씨: 춥습니다!'"
    )

    # DAG 실행 흐름 설정
    branch_task >> [task_hot, task_cold]