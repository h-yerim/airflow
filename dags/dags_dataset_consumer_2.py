import pendulum
# Airflow 3.0 부터 아래 경로로 import
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.sdk import DAG, Asset

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow import Dataset

# asset_dags_dataset_producer_1 = Asset("dags_dataset_producer_1") # 3.0
# asset_dags_dataset_producer_2 = Asset("dags_dataset_producer_2") # 3.0
dataset_dags_dataset_producer_1 = Dataset("dags_dataset_producer_1")
dataset_dags_dataset_producer_2 = Dataset("dags_dataset_producer_2")

with DAG(
        dag_id='dags_dataset_consumer_2',
        schedule=[dataset_dags_dataset_producer_1, dataset_dags_dataset_producer_2], # task 2개를 모두 바라보고있음
        start_date=pendulum.datetime(2025, 9, 4, tz='Asia/Seoul'),
        catchup=False,
        # tags=['asset','consumer'] # 3.0
) as dag:
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo {{ ti.run_id }} && echo "producer_1 와 producer_2 완료되면 수행"'
    )