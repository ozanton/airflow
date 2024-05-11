from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

task2 = PostgresOperator(
    task_id='transfer_temp',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO weather_temp (id_city, time, temp_c)
            SELECT id_city, time, temp_c FROM weatherapi_current;""",
    dag=dag)

task3 = PostgresOperator(
    task_id='transfer_hum',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO weather_hum (id_city, time, humidity)
SELECT id_city, time, humidity FROM weatherapi_current;""",
    dag=dag)

task4 = PostgresOperator(
    task_id='transfer_wind',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO weather_wind (id_city, time, wind_kph)
SELECT id_city, time, wind_kph FROM weatherapi_current;""",
    dag=dag)

task5 = PostgresOperator(
    task_id='transfer_winddir',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO weather_winddir (id_city, time, wind_direction)
SELECT id_city, time, wind_direction FROM weatherapi_current;""",
    dag=dag)

task6 = PostgresOperator(
    task_id='transfer_cloud',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO weather_cloud (id_city, time, cloud)
SELECT id_city, time, cloud FROM weatherapi_current;""",
    dag=dag)

task7 = PostgresOperator(
    task_id='clear',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM weatherapi_current""",
    trigger_rule="one_success",
    dag=dag)

task1 >> [task2, task3, task4, task5, task6] >> task7