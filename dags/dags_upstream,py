from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
import pendulum

# DAG 정의
with DAG(
    dag_id="upstream_downstream_example",
    start_date=pendulum.datetime(2024, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    # 시작 Task
    start = EmptyOperator(task_id="start")

    # 중간 Task들
    task_1 = BashOperator(
        task_id="task_1",
        bash_command="echo 'Task 1 실행'"
    )

    task_2 = BashOperator(
        task_id="task_2",
        bash_command="echo 'Task 2 실행'"
    )

    task_3 = BashOperator(
        task_id="task_3",
        bash_command="echo 'Task 3 실행'"
    )

    # 종료 Task
    end = EmptyOperator(task_id="end")

    # DAG 실행 순서 (Upstream & Downstream 관계 설정)
    start >> task_1  # start → task_1 (task_1은 start의 Downstream)
    task_1 >> [task_2, task_3]  # task_2, task_3은 task_1의 Downstream (병렬 실행)
    [task_2, task_3] >> end  # task_2, task_3 실행 후 end Task 실행