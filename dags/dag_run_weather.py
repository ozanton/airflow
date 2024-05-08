from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection("weather_postgreSQL_con")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 16),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    #"trigger_rule": "all_success",  # правило выполнения
}

dag = DAG('dag_run_weather', default_args=default_args, schedule_interval='5 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Run weatherapi", "first"])

task1 = BashOperator(
    task_id='task_run_weatherapi',
    bash_command='python3 /airflow/scripts/dag_run_weather/task_run_weatherapi.py',
    dag=dag)

temp_table = "weather_temp"
hum_table = "weather_hum"
wind_table = "weather_wind"
wind_dir_table = "weather_winddir"
cloud_table = "weather_cloud"

# SQL-запрос
sql = """
INSERT INTO {{ dag.dag_id }}."{temp_table}" (id_city, time, temp_c)
SELECT id_city, time, temp_c FROM weatherapi_current;

INSERT INTO {{ dag.dag_id }}."{hum_table}" (id_city, time, humidity)
SELECT id_city, time, humidity FROM weatherapi_current;

INSERT INTO {{ dag.dag_id }}."{wind_table}" (id_city, time, wind_kph)
SELECT id_city, time, wind_kph FROM weatherapi_current;

INSERT INTO {{ dag.dag_id }}."{wind_dir_table}" (id_city, time, wind_direction)
SELECT id_city, time, wind_direction FROM weatherapi_current;

INSERT INTO {{ dag.dag_id }}."{cloud_table}" (id_city, time, cloud)
SELECT id_city, time, cloud FROM weatherapi_current;
"""

task2 = PostgresOperator(
    task_id='task_transfer_data',
    postgres_conn_id=connection,
    sql=sql,
    dag=dag)

task3 = PostgresOperator(
    task_id='clear',
    postgres_conn_id=connection,
    sql="""DELETE FROM weatherapi_current""",
    trigger_rule="one_success",
    dag=dag)

task1 >> task2 >> task3