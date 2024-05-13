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

insert_spell = """
INSERT INTO {table_name} (id_city, timestamp, {column_name})
SELECT id_city, timestamp, {column_name} 
FROM windy_forecast
ON CONFLICT (id_city, timestamp) DO UPDATE
SET {column_name} = EXCLUDED.{column_name};
"""

task1 = BashOperator(
    task_id='task_run_windy',
    bash_command='python3 /airflow/scripts/dag_run_windy/task_run_windy.py',
    dag=dag)

task2 = PostgresOperator(
    task_id='transfer_temp',
    postgres_conn_id="weather_postgreSQL_con",
    sql=insert_spell.format(table_name='windy_temp', column_name='temp'),
    dag=dag)

task3 = PostgresOperator(
    task_id='transfer_dewpoint',
    postgres_conn_id="weather_postgreSQL_con",
    sql=insert_spell.format(table_name='windy_dewpoint', column_name='dewpoint'),
    dag=dag)

task4 = PostgresOperator(
    task_id='transfer_precip',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""
INSERT INTO windy_precip (id_city, timestamp, past3hprecip, past3hsnowprecip, past3hconvprecip)
SELECT id_city, timestamp, past3hprecip, past3hsnowprecip, past3hconvprecip 
FROM windy_forecast
ON CONFLICT (id_city, timestamp) DO UPDATE
SET past3hprecip = EXCLUDED.past3hprecip, 
    past3hsnowprecip = EXCLUDED.past3hsnowprecip, 
    past3hconvprecip = EXCLUDED.past3hconvprecip;
""",
    dag=dag)

task5 = PostgresOperator(
    task_id='transfer_wind',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""
INSERT INTO windy_wind (id_city, timestamp, wind_u, wind_v)
SELECT id_city, timestamp, wind_u, wind_v 
FROM windy_forecast
ON CONFLICT (id_city, timestamp) DO UPDATE
SET wind_u = EXCLUDED.wind_u, 
    wind_v = EXCLUDED.wind_v;
""",
    dag=dag)

task6 = PostgresOperator(
    task_id='transfer_gust',
    postgres_conn_id="weather_postgreSQL_con",
    sql=insert_spell.format(table_name='windy_gust', column_name='gust'),
    dag=dag)

task7 = PostgresOperator(
    task_id='transfer_ptype',
    postgres_conn_id="weather_postgreSQL_con",
    sql=insert_spell.format(table_name='windy_ptype', column_name='ptype'),
    dag=dag)

task8 = PostgresOperator(
    task_id='transfer_clouds',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""
INSERT INTO windy_clouds (id_city, timestamp, lclouds, mclouds, hclouds)
SELECT id_city, timestamp, lclouds, mclouds, hclouds 
FROM windy_forecast
ON CONFLICT (id_city, timestamp) DO UPDATE
SET lclouds = EXCLUDED.lclouds, 
    mclouds = EXCLUDED.mclouds, 
    hclouds = EXCLUDED.hclouds;
""",
    dag=dag)

task9 = PostgresOperator(
    task_id='transfer_rh',
    postgres_conn_id="weather_postgreSQL_con",
    sql=insert_spell.format(table_name='windy_rh', column_name='rh'),
    dag=dag)

task10 = PostgresOperator(
    task_id='transfer_pressure',
    postgres_conn_id="weather_postgreSQL_con",
    sql=insert_spell.format(table_name='windy_pressure', column_name='pressure'),
    dag=dag)

task11 = PostgresOperator(
    task_id='clear',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM windy_forecast""",
    trigger_rule="one_success",
    dag=dag)

task1 >> [task2, task3, task4, task5, task6, task7, task8, task9, task10] >> task11