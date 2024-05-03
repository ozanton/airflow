from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from dag_ratescb_plugin.csvcbrf_to_db import AgaOperator
from dag_ratescb_plugin.csvnbrs_to_db import Aga1Operator
from dag_ratescb_plugin.datacbrf import get_rates_cbrf, get_buffer_cbrf
from dag_ratescb_plugin.datanbrs import get_currency_nbrs, get_buffer_nbrs
import json

connection = BaseHook.get_connection("exchrate_postgreSQL_con")

def data_to_xcom(**kwargs):
    ti = kwargs['ti']
    try:
        data = get_rates_cbrf()
        buffer_content = get_buffer_cbrf(data)
        if buffer_content:
            json_data = json.loads(buffer_content)
            ti.xcom_push(key='data', value=json_data)
            ti.log.info("Данные успешно переданы в XCom")
            return "Data successfully pushed to XCom"
        else:
            ti.log.error("Полученные данные пусты")
            return "Received data is empty"
    except Exception as e:
        ti.log.exception("Произошла ошибка при получении данных и передаче в XCom: %s", str(e))
        return "Failed to push data to XCom"

def data_load(**kwargs):
    ti = kwargs['ti']
    xcom_data = ti.xcom_pull(key='data', task_ids='task_ratescbrf')
    print("Received XCom data:", xcom_data)

    if isinstance(xcom_data, list) and all(isinstance(item, dict) for item in xcom_data):
        print("XCom data is a list of dictionaries.")
    else:
        print("XCom data is not in the expected format. It should be a list of dictionaries.")
        return

    return xcom_data

def nbrs_data_to_xcom(**kwargs):
    ti = kwargs['ti']
    try:
        data_value = get_currency_nbrs()
        buffer_content = get_buffer_nbrs(data_value)
        if buffer_content:
            json_value = json.loads(buffer_content)
            ti.xcom_push(key='data_nbrs', value=json_value)
            ti.log.info("Данные успешно переданы в XCom")
            return "Data successfully pushed to XCom"
        else:
            ti.log.error("Полученные данные пусты")
            return "Received data is empty"
    except Exception as e:
        ti.log.exception("ошибка при получении данных и передаче в XCom: %s", str(e))
        return "Failed to push data to XCom"

def nbrs_data_load(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(key='data_nbrs', task_ids='task_ratesnbrs')
    print("Received XCom data:", xcom_value)
    # Проверяем, что данные из XCom являются списком словарей
    if isinstance(xcom_value, list) and all(isinstance(item, dict) for item in xcom_value):
        print("XCom data is a list of dictionaries.")
    else:
        print("XCom data is not in the expected format. It should be a list of dictionaries.")
        return
    return xcom_value

table_name_task2 = 'rates_cbrf'
default_mapping_task2 = {}

table_name_task4 = 'rates_nbrs'
default_mapping_task4 = {}

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 22),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    #"trigger_rule": "all_success",  # правило выполнения
}

dag = DAG('dag_ratescb', default_args=default_args, schedule_interval='0 9 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["ratescb", "third"])

task1 = PythonOperator(
    task_id='task_ratescbrf',
    python_callable=data_to_xcom,
    provide_context=True,
    dag=dag)

task2 = AgaOperator(
    task_id='load_cbrf_db',
    postgre_conn=connection,
    python_callable=data_load,
    table_name='rates_cbrf',
    dag=dag)

task3 = PythonOperator(
    task_id='task_ratesnbrs',
    python_callable=nbrs_data_to_xcom,
    provide_context=True,
    dag=dag)

task4 = Aga1Operator(
    task_id='load_nbrs_db',
    postgre_conn=connection,
    python_callable=nbrs_data_load,
    table_name=table_name_task4,
    default_mapping=default_mapping_task4,
    dag=dag)

task1 >> task2 >> task3 >> task4