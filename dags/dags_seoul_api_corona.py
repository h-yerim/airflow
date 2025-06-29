from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator #plugins 경로는 생략가능
import pendulum
# # Airflow 3.0 부터 아래 경로로 import
# from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import
from airflow import DAG

with DAG(
    dag_id='dags_seoul_api_corona',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    '''서울시 코로나19 확진자 발생동향'''
    tb_corona19_count_status = SeoulApiToCsvOperator(
        task_id='tb_corona19_count_status',
        dataset_nm='TbCorona19CountStatus', #서울시 코로나19 확진자 발생동향의 오픈api명세에 나온 데이터셋이름
        # path는 배치 날짜마다 디렉토리를 바꾸게하려는 용도로 만들어놓음
        path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}', #ds_nodash:yyyymmdd 형식
        file_name='TbCorona19CountStatus.csv'
    )
    
    '''서울시 코로나19 백신 예방접종 현황'''
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id='tv_corona19_vaccine_stat_new',
        dataset_nm='tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}',
        file_name='tvCorona19VaccinestatNew.csv'
    )

    tb_corona19_count_status >> tv_corona19_vaccine_stat_new