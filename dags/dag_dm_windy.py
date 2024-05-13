from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 16),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    #"trigger_rule": "all_success",  # правило выполнения
}

dag = DAG('dag_dm_windy', default_args=default_args, schedule_interval='0 0 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["data mart", "fifth"])

task1 = PostgresOperator(
    task_id='clear',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM windy_dm_daily""",
    dag=dag)

task2 = PostgresOperator(
    task_id='windy_daily',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""_""",
    dag=dag)

task3 = PostgresOperator(
    task_id='dm_all',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""_""",
    dag=dag)

task1 >> task2 >> task3