import pendulum
# Airflow 3.0 부터 아래 경로로 import
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.sdk import DAG, Asset

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow import Dataset

# asset_dags_asset_producer_1 = Asset("dags_dataset_producer_1") # 3.0버전
dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1") # publish 했떤 key값을 꺼내서 객체 생성

with DAG(
        dag_id='dags_dataset_consumer_1',
        schedule=[dataset_dags_dataset_producer_1], # key를 구독한다는 의미: 스케줄을 가지고있지않고 publish한 task가 완료되면 이 task도 수행을 진행
        start_date=pendulum.datetime(2025, 9, 4, tz='Asia/Seoul'),
        catchup=False,
        # tags=['asset','consumer'] # 3.0
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1 이 완료되면 수행"'
    )