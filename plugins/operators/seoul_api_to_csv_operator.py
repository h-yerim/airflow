from airflow.hooks.base import BaseHook
import pandas as pd
# # Airflow 3.0 부터 아래 경로로 import
# from airflow.models import BaseOperator

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import
from airflow.models.baseoperator import BaseOperator

class SeoulApiToCsvOperator(BaseOperator): #custom operator
    template_fields = ('endpoint', 'path','file_name','base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/' + dataset_nm
        self.base_dt = base_dt

    # url:8080/apikey/type(xml인지 json인지)/dataset_nm/start/end 이렇게 endpoint를 줘야 데이터를 가져올수있음 start와 end를 지정안한건 전체 데이터 가져오기위함
    def execute(self, context): 
        import os
        
        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}' #start와 end만 빼고 url 작성됨

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000 #1000건까지 가져오고 만약 데이터가 더 있다면 그 다음 돌때 1001번부터 2000까지 가져오도록 그다음 2001~ 이런식으로 1000건씩 가져와서 더이상 없으면 끝내는 반복문
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝:{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            total_row_df = pd.concat([total_row_df, row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path): #csv로 저장하기 위해씀
            os.system(f'mkdir -p {self.path}') #self.path에 디렉토리가 없으면 디렉토리 생성
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self, base_url, start_row, end_row):
        import requests
        import json 

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }

        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'
        response = requests.get(request_url, headers)
        contents = json.loads(response.text) #딕셔너리값(dags_simplt_http_operator 실행값 참고)

        key_nm = list(contents.keys())[0] 
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        return row_df