import sys
import os
import requests
import xmltodict
import json
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException

#config import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import WORK_NET_URL, JOB_INFO_END_POINT, JOB_INFO_AUTH_KEY

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 11),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#직업정보 API 호출
def call_api(**kwargs):
    api_url = f"{WORK_NET_URL}{JOB_INFO_END_POINT}?authKey={JOB_INFO_AUTH_KEY}&returnType=XML&target=JOBCD"
    
    response = requests.get(api_url)
    if response.status_code == 200:
        # 바이트를 문자열로 디코딩
        content_str = response.content.decode('utf-8')
        kwargs['ti'].xcom_push(key='api_response', value=content_str)
        print("API 호출 성공")
    else:
        print(f"API 호출 실패. 상태 코드: {response.status_code}")
        raise AirflowException("API 호출 실패")

#json 파싱
def parse_xml_to_json(**kwargs):
    ti = kwargs['ti']
    xml_content = ti.xcom_pull(task_ids='call_api', key='api_response')
    
    if xml_content:
        xml_dict = xmltodict.parse(xml_content)
        json_data = json.loads(json.dumps(xml_dict, ensure_ascii=False))
        
        # jobsList에서 jobList 항목만 추출
        if 'jobsList' in json_data and 'jobList' in json_data['jobsList']:
            job_list = json_data['jobsList']['jobList']
            # 리스트가 아닌 경우 리스트로 변환
            if not isinstance(job_list, list):
                job_list = [job_list]
            kwargs['ti'].xcom_push(key='parsed_response', value=job_list)
            print(json.dumps(job_list, ensure_ascii=False, indent=2))
        else:
            print("jobList를 찾을 수 없습니다.")
            raise AirflowException("jobList 데이터 없음")
    else:
        print("API 응답 데이터를 찾을 수 없습니다.")
        raise AirflowException("API 응답 데이터 없음")

def call_upsert_api(**kwargs):
    ti = kwargs['ti']
    job_list = ti.xcom_pull(task_ids='parse_xml_to_json', key='parsed_response')
    
    # JSON 객체로 감싸서 변환
    json_data = json.dumps({"jobList": job_list}, ensure_ascii=False)
    print("전송할 데이터:", json_data)

    # 내부 API(DB upsert) 호출 
    url = "sampleurl"  # 나중에 자바로 API개발하면 연결하기
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, data=json_data.encode('utf-8'), headers=headers)
        response.raise_for_status()  

        if response.status_code == 201:
            data = response.json()
            print("API 응답 데이터:", json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print(f"예상치 못한 상태 코드: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        raise AirflowException("Upsert API 호출 실패")

dag = DAG(
    'JOB_INFO_DAG',
    default_args=default_args,
    description='call JOB_INFO_API and upsert to DB',
    schedule_interval='0 0 * * *',
    catchup=False,
)

t_start = DummyOperator(task_id='start', dag=dag)
t_end = DummyOperator(task_id='end', dag=dag)

t_call_api = PythonOperator(
    task_id='call_api',
    python_callable=call_api,
    dag=dag,
)

t_parse_xml_to_json = PythonOperator(
    task_id='parse_xml_to_json',
    python_callable=parse_xml_to_json,
    dag=dag,
)

t_call_upsert_api = PythonOperator(
    task_id='t_call_upsert_api',
    python_callable=call_upsert_api,
    dag=dag,
)

t_start >> t_call_api >> t_parse_xml_to_json >> t_call_upsert_api >> t_end
