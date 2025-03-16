from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum

# 자식 DAG 정의
with DAG(
    dag_id="child_dag",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as child_dag:

    # 자식 DAG 시작
    start = EmptyOperator(task_id="start")

    # 주요 작업 수행
    task_1 = EmptyOperator(task_id="task_1")
    task_2 = EmptyOperator(task_id="task_2")

    # 자식 DAG 종료
    end = EmptyOperator(task_id="end")

    # 실행 순서 설정
    start >> [task_1, task_2] >> end
