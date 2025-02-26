from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template0",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        # "" 안의 내용이 echo(출력)할 내용
        bash_command = 'echo "data_interval_end: {{data_interval_end}} "'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {
            # | ds하면 타임스탬프가아닌 dash 형태로 출력됨
            'START_DATE' : '{{data_interval_start | ds}}',
            'END_DATE' : '{{data_interval_end | ds}}',
        },
        # && : 쉘스크립트 문법에서는 앞의 command가 성공하면 뒤의 command도 실행하겠다는 의미
        bash_command = 'echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2