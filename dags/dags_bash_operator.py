from airflow.models.dag import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    # airflow 처음 들어왔을때 보이는 DAG의 이름을 의미(화면에서보이는 이름들 *python파일명과는 상관없음, 그러나 일반적으로 파일명과 일치시키는게 좋음. 나중에 수정시 dag이 많으면 찾기힘듦)
    dag_id="dags_bash_operator", 
    # cron 스케줄, 이 dag의 주기 설정(분/시/일/월/요일 설정) 지금은 매일 0시0분에 도는작업
    schedule="0 0 * * *",
    # dag이 언제부터 돌건지를 설정 tz는 timezone, UTC는 세계표준시로 한국보다 9시간 느림 -> Asia/Seoul로 무조건 바꿈
    start_date=pendulum.datetime(2024, 1, 29, tz="Asia/Seoul"),
    # ex) 오늘이 3/1이고 start_date가 1/1일 경우 1/1~3/1까지 누락된것도 같이 돌릴거냐를 결정(True: 돌림, false:안돌림)
    # 주의해야할것은 순서대로가 아닌 한번에 돌리기 때문에 문제가 생길수있음. 그래서 보통은 False로 둠.
    catchup=False

) as dag:
     # task는 오퍼레이터를 통해 만들어짐
     # bash_t1 : task 객체명임
     bash_t1 = BashOperator(
     # 그래프에는 객체명과는 상관없이 task_id가 나옴, 그러나 나중에 찾기쉽게 객체명과 task_id를 맞춰주는게 편함
        task_id="bash_t1", # task_id
        bash_command="echo whoami", # 어떤 shell 스크립트를 수행할거냐는 것, echo는 print와 동일한 기능으로 생각하면됨
    )
     # task 하나 더 생성
     bash_t2 = BashOperator(
        task_id="bash_t2", 
        bash_command="echo $HOSTNAME", 
    )
     
     # 실행순서
     bash_t1 >> bash_t2