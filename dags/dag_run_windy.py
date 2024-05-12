from datetime import datetime, timedelta
from airflow import DAG
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

dag = DAG('dag_run_windy', default_args=default_args, schedule_interval='10 1/3 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Run windy", "second"])

task1 = BashOperator(
    task_id='task_run_windy',
    bash_command='python3 /airflow/scripts/dag_run_windy/task_run_windy.py',
    dag=dag)

task2 = PostgresOperator(
    task_id='transfer_temp',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_temp (id_city, timestamp, temp)
SELECT id_city, timestamp, temp FROM windy_forecast""",
    dag=dag)

task3 = PostgresOperator(
    task_id='transfer_dewpoint',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_dewpoint (id_city, timestamp, dewpoint)
SELECT id_city, timestamp, dewpoint FROM windy_forecast""",
    dag=dag)

task4 = PostgresOperator(
    task_id='transfer_precip',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_precip (id_city, timestamp, past3hprecip, past3hsnowprecip, past3hconvprecip)
SELECT id_city, timestamp, past3hprecip, past3hsnowprecip, past3hconvprecip FROM windy_forecast;""",
    dag=dag)

task5 = PostgresOperator(
    task_id='transfer_wind',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_wind (id_city, timestamp, wind_u, wind_v)
SELECT id_city, timestamp, wind_u, wind_v FROM windy_forecast;""",
    dag=dag)

task6 = PostgresOperator(
    task_id='transfer_gust',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_gust (id_city, timestamp, gust)
SELECT id_city, timestamp, gust FROM windy_forecast;""",
    dag=dag)

task7 = PostgresOperator(
    task_id='transfer_ptype',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_ptype (id_city, timestamp, ptype)
SELECT id_city, timestamp, ptype FROM windy_forecast;""",
    dag=dag)

task8 = PostgresOperator(
    task_id='transfer_clouds',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_clouds (id_city, timestamp, lclouds, mclouds, hclouds)
SELECT id_city, timestamp, lclouds, mclouds, hclouds FROM windy_forecast;""",
    dag=dag)

task9 = PostgresOperator(
    task_id='transfer_rh',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_rh (id_city, timestamp, rh)
SELECT id_city, timestamp, rh FROM windy_forecast;""",
    dag=dag)

task10 = PostgresOperator(
    task_id='transfer_pressure',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_pressure (id_city, timestamp, pressure)
SELECT id_city, timestamp, pressure FROM windy_forecast;""",
    dag=dag)

task11 = PostgresOperator(
    task_id='clear',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM windy_forecast""",
    trigger_rule="one_success",
    dag=dag)

task1 >> [task2, task3, task4, task5, task6, task7, task8, task9, task10] >> task11