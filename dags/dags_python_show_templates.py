from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates", 
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2025, 2, 20, tz="Asia/Seoul"),
    catchup=True   # True 옵션 주면 2/20 - 현재날짜까지 구간을 모두 수행하겠다는 것
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()