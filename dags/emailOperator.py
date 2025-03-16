from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="email_operator_example",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    
    send_email = EmailOperator(
        task_id="send_email",
        to="shkang991@gmail.com",  # 수신자 이메일 주소
        subject="Airflow DAG 실행 완료",
        html_content="""
        <h3>Airflow DAG이 성공적으로 실행되었습니다!</h3>
        <p>이메일 알림을 정상적으로 전송합니다.</p>
        """,
    )