from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator", 
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2024, 1, 29, tz="Asia/Seoul"),
    catchup=False
) as dag:
    # 파이썬 오퍼레이터는 함수를 실행하기때문에 함수가 있어야함
    def select_fruit():
        fruit = ['APPLE','BANANA','ORANGE','AVOCADO']
        rand_int = random.randint(0,3) # 0~3중 임의의 int값 return
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = 'py_t1',
        python_callable = select_fruit # 어떤 함수를 실행시킬거냐
    )

    py_t1 # task 1개이기때문