from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator



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

task1