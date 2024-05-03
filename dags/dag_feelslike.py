from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook




connection = BaseHook.get_connection("weather_postgreSQL_con")