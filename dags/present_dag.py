from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

# 부모 DAG 정의
with DAG(
    dag_id="parent_dag",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as parent_dag:

    # 부모 DAG 시작
    start = EmptyOperator(task_id="start")

    # 자식 DAG 실행 트리거
    trigger_child_dag = TriggerDagRunOperator(
        task_id="trigger_child_dag",
        trigger_dag_id="child_dag",  # 실행할 자식 DAG ID
        wait_for_completion=True  # 자식 DAG 완료까지 대기
    )

    # 부모 DAG 종료
    end = EmptyOperator(task_id="end")

    # DAG 실행 흐름 설정
    start >> trigger_child_dag >> end