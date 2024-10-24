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
from config import WORK_NET_URL, OPEN_RECRUITMENT_END_POINT, OPEN_RECRUITMENT_AUTH_KEY

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
def call_api_and_parse_xml_to_json(**kwargs):
    start_page = 1    
    all_data = []  # 모든 데이터를 저장할 리스트
    
    while True:
        api_url = f"{WORK_NET_URL}{OPEN_RECRUITMENT_END_POINT}?authKey={OPEN_RECRUITMENT_AUTH_KEY}&callTp=L&returnType=XML&startPage={start_page}&display=100"
        
        response = requests.get(api_url)
        if response.status_code == 200:
            # XML을 문자열로 디코딩
            xml_content = response.content.decode('utf-8')
            # print("API 응답 XML:", xml_content)  # XML 내용 출력
            # JSON으로 파싱
            json_data = parse_xml_to_json(xml_content)
            
            if json_data is not None:
                # JSON 데이터를 리스트에 추가
                all_data.append(json_data)
                print(f"API 호출 성공 (start_page: {start_page})")
                start_page += 1
            else:
                print(f"유효한 데이터 없음 (start_page: {start_page})")
                #start_page을 1 올렸으나 데이터가 없을 경우 더이상 읽어올게 없다고 판단. 루프문 종료                
                print("모든 데이터 처리. 루프 종료.")
                break                                 
        else:
            print(f"API 호출 실패. 상태 코드: {response.status_code}")
            raise AirflowException("API 호출 실패")
    
    all_data_json = json.dumps(all_data, ensure_ascii=False)
    kwargs['ti'].xcom_push(key='api_response', value=all_data_json)
    print(f"XCom에 {len(all_data)}페이지의 데이터 저장")

#json 파싱
def parse_xml_to_json(xml_content):
    xml_dict = xmltodict.parse(xml_content)
    
    if 'dhsOpenEmpInfoList' in xml_dict and 'dhsOpenEmpInfo' in xml_dict['dhsOpenEmpInfoList']:
        emp_info = xml_dict['dhsOpenEmpInfoList']['dhsOpenEmpInfo']
                
        if not isinstance(emp_info, list):
            emp_info = [emp_info]
        
        print("공채속보 데이터 있음. dhsOpenEmpInfo 배열을 JSON으로 반환.")
        return emp_info
    else:
        print("공채속보 데이터 없음.")
        return None
    

def call_upsert_api(**kwargs):
    ti = kwargs['ti']
    all_data_json = ti.xcom_pull(task_ids='t_call_api_and_parse_xml_to_json', key='api_response')    
    
    # JSON 문자열을 파이썬 객체로 변환
    all_data = json.loads(all_data_json)
    
    # 데이터를 dhsOpenEmpInfoList 키로 감싸기 (배열을 한 번만 감싸도록 수정)
    wrapped_data = {"dhsOpenEmpInfoList": [item for sublist in all_data for item in sublist]}
    
    # 다시 JSON 문자열로 변환
    wrapped_json = json.dumps(wrapped_data, ensure_ascii=False)
    
    print("전송할 데이터:", wrapped_json)
    
    # 내부 API(DB upsert) 호출 
    url = "sampleurl"  # 나중에 자바로 API개발하면 연결하기
    headers = {'Content-Type': 'application/json'} 

    try:
        response = requests.post(url, data=wrapped_json.encode('utf-8'), headers=headers)
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
    'OPEN_RECRUITMENT_DAG',
    default_args=default_args,
    description='call OPEN_RECRUITMENT and upsert to DB',
    schedule_interval='0 * * * *',
    catchup=False,
)

t_start = DummyOperator(task_id='start', dag=dag)
t_end = DummyOperator(task_id='end', dag=dag)

t_call_api_and_parse_xml_to_json = PythonOperator(
    task_id='t_call_api_and_parse_xml_to_json',
    python_callable=call_api_and_parse_xml_to_json,
    dag=dag,
)

t_call_upsert_api = PythonOperator(
    task_id='t_call_upsert_api',
    python_callable=call_upsert_api,
    provide_context=True,
    dag=dag,
)

t_start >> t_call_api_and_parse_xml_to_json >> t_call_upsert_api >> t_end
