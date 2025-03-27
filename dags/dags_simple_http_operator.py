# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2025, 3, 28, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:

    '''서울시 공공자전거 대여소 정보'''
    # SimpleHttpOperator -> HttpOperator로 변경되었습니다.
    # HttpOperator로 작성해주세요.
    tb_cycle_station_info = HttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleStationInfo/1/10/',
        method='GET',
        headers={'Content-Type': 'application/json', # json 타입으로 받음
                        'charset': 'utf-8',
                        'Accept': '*/*'
                        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs): # http 오퍼레이터가 가지고 온 값이 어떻게 나오는지 보여주는 용도
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info') # ti객체에서 xcom값 땡겨옴(사실상 task이므로 http오퍼레이터의 return값 가져옴.)
        import json
        from pprint import pprint

        pprint(json.loads(rslt))
        
    tb_cycle_station_info >> python_2()