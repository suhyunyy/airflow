from airflow import DAG
import pendulum
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.email import send_email

def failure_callback(context):
    email_operator = EmailOperator(
        task_id='send_failure_email',
        to='shkang991@gmail.com',
        subject='Airflow Task 실패 알림',
        html_content=f"""
        <h3>Task 실패 알림</h3>
        <p>Dag ID: {context['dag'].dag_id}</p>
        <p>Task ID: {context['task_instance'].task_id}</p>
        <p>Execution Date: {context['execution_date']}</p>
        <p>Log URL: <a href='{context['task_instance'].log_url}'>로그 확인</a></p>
        """
    )
    email_operator.execute(context)

with DAG(
    dag_id="dags_email_operator_failure_alert",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')
    
    failing_task = DummyOperator(
        task_id='failing_task',
        on_failure_callback=failure_callback
    )
    
    start >> failing_task
