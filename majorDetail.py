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
from config import WORK_NET_URL, MAJOR_DETAIL_END_POINT, MAJOR_DETAIL_AUTH_KEY

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
    emp_curt_state1_id = 1
    emp_curt_state2_id = 1
    all_data = []  # 모든 데이터를 저장할 리스트
    
    while True:
        api_url = f"{WORK_NET_URL}{MAJOR_DETAIL_END_POINT}?authKey={MAJOR_DETAIL_AUTH_KEY}&returnType=XML&target=MAJORDTL&majorGb=1&empCurtState1Id={emp_curt_state1_id}&empCurtState2Id={emp_curt_state2_id}"
        
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
                print(f"API 호출 성공 (empCurtState1Id: {emp_curt_state1_id}, empCurtState2Id: {emp_curt_state2_id})")
                emp_curt_state2_id += 1
            else:
                print(f"유효한 데이터 없음 (empCurtState1Id: {emp_curt_state1_id}, empCurtState2Id: {emp_curt_state2_id})")
                #state1을 1 올렸으나(state2는 1로 초기화) 데이터가 없을 경우 더이상 읽어올게 없다고 판단. 루프문 종료
                if emp_curt_state2_id == 1:
                    print("모든 데이터 처리. 루프 종료.")
                    break 
                emp_curt_state2_id = 1
                emp_curt_state1_id += 1
        else:
            print(f"API 호출 실패. 상태 코드: {response.status_code}")
            raise AirflowException("API 호출 실패")
    
    all_data_json = json.dumps(all_data, ensure_ascii=False)
    kwargs['ti'].xcom_push(key='api_response', value=all_data_json)
    print(f"XCom에 {len(all_data)}개의 데이터 저장")

#json 파싱
def parse_xml_to_json(xml_content):
    xml_dict = xmltodict.parse(xml_content)
    
    if 'majorSum' in xml_dict:
        major_sum = xml_dict['majorSum']
        
        result = {
            'knowDptNm': major_sum.get('knowDptNm'),
            'knowSchDptNm': major_sum.get('knowSchDptNm'),
            'knowDptId': major_sum.get('knowDptId'),
            'knowSchDptId': major_sum.get('knowSchDptId'),
            'schDptIntroSum': major_sum.get('schDptIntroSum')
        }
        
        # licList(관련 자격증) 처리
        if 'licList' in major_sum:
            if isinstance(major_sum['licList'], list):
                result['adoptCertCont'] = major_sum['licList'][0].get('adoptCertCont') if major_sum['licList'] else None
            elif isinstance(major_sum['licList'], dict):
                result['adoptCertCont'] = major_sum['licList'].get('adoptCertCont')
            else:
                result['adoptCertCont'] = None
        else:
            result['adoptCertCont'] = None
        
        # relAdvanJobsList(관련 직업) 처리 - 모든 knowJobNm들의 값을 콤마로 연결해서 재조합
        if 'relAdvanJobsList' in major_sum:
            if isinstance(major_sum['relAdvanJobsList'], list):
                know_job_names = [job['knowJobNm'] for job in major_sum['relAdvanJobsList'] if 'knowJobNm' in job]
            elif isinstance(major_sum['relAdvanJobsList'], dict):
                know_job_names = [major_sum['relAdvanJobsList']['knowJobNm']] if 'knowJobNm' in major_sum['relAdvanJobsList'] else []
            else:
                know_job_names = []
            
            result['knowJobNm'] = ', '.join(know_job_names) if know_job_names else None
        else:
            result['knowJobNm'] = None
        
        # 관련 자격증 또는 관련 직업 중 하나라도 있으면 결과 반환
        if result['adoptCertCont'] is not None or result['knowJobNm'] is not None:
            print("관련 자격증 또는 관련 직업 데이터 있음. 데이터 반환.")
            return result
        else:
            print("관련 자격증과 관련 직업 데이터 모두 없음.")
            return None
    else:
        print("<majorSum> 태그 없음.")
        return None
    

def call_upsert_api(**kwargs):
    ti = kwargs['ti']
    all_data_json = ti.xcom_pull(task_ids='t_call_api_and_parse_xml_to_json', key='api_response')    
    
    # JSON 문자열을 파이썬 객체로 변환
    all_data = json.loads(all_data_json)
    
    # 데이터를 majorDetailList 키로 감싸기
    wrapped_data = {"majorDetailList": all_data}
    
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
    'MAJOR_DETAIL_DAG',
    default_args=default_args,
    description='call MAJOR_DETAIL and upsert to DB',
    schedule_interval='0 0 * * *',
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
