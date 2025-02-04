from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1", # 매월 첫번째주 토요일 0시 10분에 도는 작업
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    t1_orange = BashOperator(
        task_id="t1_orange",
        bash_command="/opt/airflow/plugins/shell/test.sh ORANGE", # task 실행: 워커컨테이너, 따라서 워커 컨테이너가 shell 프로그램 위치 알수있게 작성(이걸 위해 volumes에서 작업한것)
    )

    t2_avocado = BashOperator(
        task_id="t2_avocado",
        bash_command="/opt/airflow/plugins/shell/test.sh AVOCADO",
    )

    t1_orange >> t2_avocado