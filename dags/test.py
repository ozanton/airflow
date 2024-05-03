import datetime
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from dag_ratescb_plugin.csvcbrf_to_db import AgaOperator
from dag_ratescb_plugin.csvnbrs_to_db import Aga1Operator
from pycbrf import ExchangeRates
import requests
import pandas as pd
import io
import csv

connection = BaseHook.get_connection("exchrate_postgreSQL_con")
def get_rates_cbrf():
    #  дата: запуск ежедневно для now - 9.00 утра и tomorrow -  15.00 дня
    date_str = str(datetime.now())[:10]
    tomorrow_str = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    # запрос
    rates = ExchangeRates(date_str, locale_en=False)
    # список валют
    currencies = ['USD', 'EUR', 'RSD', 'BAM', 'AMD', 'GEL', 'KZT']
    data = {
      'date': date_str,
      'name': [],
      'code': [],
      'num': [],
      'rate': [],
    }

    for code in currencies:
      # курс по коду валюты
      rate = rates[code]

      # наличие данных
      if rate:
        data['name'].append(rate.name)
        data['code'].append(rate.code)
        data['num'].append(rate.num)
        data['rate'].append(rate.rate)

    df = pd.DataFrame(data)

    # DataFrame
    print(df.to_string())

    buffer = io.StringIO()
    df.to_csv(buffer, index=False, sep=';', na_rep='NUL', quoting=csv.QUOTE_MINIMAL,
              header=True, float_format='%.8f', doublequote=False, escapechar='\\')
    buffer.seek(0)

    # Доступ к данным в буфере
    buffer_content = buffer.getvalue()
    print("Данные DataFrame в буфере:\n", buffer_content)

    # Очистка буфера
    buffer.close()
    print("Файл не сохранялся, данные в буфере.")

    return buffer_content

def data_to_xcom(**kwargs):
    buffer_content = get_rates_cbrf()
    kwargs['ti'].xcom_push(key='csv_buffer', value=buffer_content)

def get_rates_nbrs(currencies):
    """ Получает коды валют для заданного списка валют (основной апи c курсом дает ошибочный код валюты)"""
    try:
        response = requests.get('https://kurs.resenje.org/api/v1/currencies')
        data = response.json()

        if not isinstance(data, dict) or "currencies" not in data:
            print("Не удалось обработать ответ сервера.")
            return {}

        rates = {}
        for currency in data['currencies']:
            code = currency['code']
            if code in currencies:
                rates[code] = currency['number']

        return rates
    except requests.exceptions.RequestException as e:
        print(f"Ошибка получения данных: {e}")
        return {}

def get_detailed_data(currency):
    """
    Получает данные о курсах валют для конкретной валюты.
    """
    try:
        response = requests.get(f'https://kurs.resenje.org/api/v1/currencies/{currency}/rates/today')
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Ошибка получения данных: {e}")
        return {}

def get_exchange_rates_data():
    currencies = ['EUR', 'USD', 'BAM', 'NOK', 'HUF', 'GBP', 'CZK', 'RUB']

    # получение number
    exchange_rates = get_rates_nbrs(currencies)

    data_list = []

    # данные для каждой валюты
    for currency in currencies:
        detailed_data = get_detailed_data(currency)
        if detailed_data:
            detailed_data['number'] = exchange_rates.get(currency)
            data_list.append(detailed_data)

    # проверка данных
    if data_list:
        df_columns = ['date', 'code', 'number', 'parity', 'exchange_buy', 'exchange_middle', 'exchange_sell']
        df = pd.DataFrame(data_list, columns=df_columns)
        return df.to_csv(index=False, sep=';', na_rep='NUL', quoting=csv.QUOTE_MINIMAL,
                         header=True, float_format='%.8f', doublequote=False, escapechar='\\')
    else:
        print("Не удалось собрать данные о курсах валют.")
        return None

def nbrs_data_to_xcom(**kwargs):
    buffer_content = get_exchange_rates_data()
    if buffer_content:
        kwargs['ti'].xcom_push(key='csv_buffer', value=buffer_content)

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
    task_id='load_csv_to_cbrf',
    postgre_conn=connection,
    csv_buffer="{{ ti.xcom_pull(task_ids='task_ratescbrf', key='csv_buffer') }}",
    table_name=table_name_task2,
    default_mapping=default_mapping_task2,
    dag=dag)

task3 = PythonOperator(
    task_id='task_ratesnbrs',
    python_callable=nbrs_data_to_xcom,
    provide_context=True,
    dag=dag)

task4 = Aga1Operator(
    task_id='load_csv_to_nbrs',
    postgre_conn=connection,
    csv_buffer="{{ ti.xcom_pull(task_ids='task_ratesnbrs', key='csv_buffer') }}",
    table_name=table_name_task4,
    default_mapping=default_mapping_task4,
    dag=dag)

task1 >> task2 >> task3 >> task4

# def data_load(**kwargs):
#     ti = kwargs['ti']
#     xcom_data = ti.xcom_pull(key='data', task_ids=['task_ratescbrf'])
#     print(xcom_data)
#     if xcom_data:
#         csv_string = io.StringIO()
#         writer = csv.writer(csv_string, delimiter=';')
#         writer.writerows(xcom_data)
#         return csv_string.getvalue()
#     else:
#         return ""