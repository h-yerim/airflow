from airflow import DAG
import datetime
import pendulum
from airflow.operators.email import EmailOperator

with DAG(
     dag_id = "dags_email_operator",
     schedule = '0 8 1 * *',
     start_date = pendulum.datetime(2025,2,6,tz="Asia/Seoul"),
     catchup = False
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to = 'hrim1103@hanmail.net', # 받을 이메일주소 
        # cc = '참조받을사람 이메일 계정' 
        subject = 'Airflow 성공메일',
        html_content='Airflow 작업이 완료되었습니다'
    )