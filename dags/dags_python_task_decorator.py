from airflow import DAG
import pendulum 
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 2, 25, tz="Asia/Seoul"),
    catchup=False,
)as dag:

    @task(task_id="python_task_1") # python operator 만들때 주던 task_id와 동일하다고 보면됨
    def print_context(some_input):
        print(some_input)

    # task명과 task 객체 이름 일치시키는게 좋음
    python_task_1 = print_context("task_decorator 실행")

