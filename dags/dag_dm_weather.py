from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 25),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    #"trigger_rule": "all_success",  # правило выполнения
}

dag = DAG('dag_dm_weather', default_args=default_args, schedule_interval='0 0 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["data mart", "fourth"])

task1 = PostgresOperator(
    task_id='clear',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM weather_dm_daily""",
    dag=dag)

task2 = PostgresOperator(
    task_id='weather_daily',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO weather_dm_daily(date, name_city, min_temp_c, max_temp_c, avg_humidity, avg_wind_ms, prevailing_wind_direction, prevailing_wind_direction_str, prevailing_cloudiness_percent, prevailing_cloudiness_type)
WITH 
cloudiness_mapping AS (
  SELECT 
    unnest(ARRAY[0, 25, 75, 100]) AS cloudiness_percent,
    unnest(ARRAY['Clear', 'Partly Cloudy', 'Cloudy', 'Overcast']) AS cloudiness_type
),
daily_weather AS (
  SELECT 
    DATE(wt."time") AS date,
    dc.name_city,
    MIN(wt.temp_c) AS min_temp_c,
    MAX(wt.temp_c) AS max_temp_c,
    ROUND(AVG(wh.humidity)::numeric, 2) AS avg_humidity,
    ROUND(AVG((ww.wind_kph * 0.27777777777777777777777777777778))::numeric, 2) AS avg_wind_ms,
    CASE 
      WHEN MAX(wd.wind_direction) = 0 THEN 'Calm'
      ELSE (
        SELECT direction
        FROM (
          SELECT 
            unnest(ARRAY[0, 45, 90, 135, 180, 225, 270, 315, 360]) AS degrees,
            unnest(ARRAY['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW','N']) AS direction
        ) AS wind_direction_mapping
        WHERE degrees <= MAX(wd.wind_direction)
        ORDER BY degrees DESC
        LIMIT 1
      )
    END AS prevailing_wind_direction_str,
    MAX(wd.wind_direction) AS prevailing_wind_direction,
    (SELECT cloudiness_type
     FROM cloudiness_mapping cm
     WHERE cm.cloudiness_percent <= MAX(wc.cloud)
     ORDER BY cm.cloudiness_percent DESC
     LIMIT 1) AS prevailing_cloudiness_type,
    MAX(wc.cloud) AS prevailing_cloudiness_percent
  FROM weather_temp wt
  JOIN dict_cities dc ON wt.id_city = dc.id_city
  JOIN weather_hum wh ON wt.id_city = wh.id_city AND DATE(wt."time") = DATE(wh."time")
  JOIN weather_wind ww ON wt.id_city = ww.id_city AND DATE(wt."time") = DATE(ww."time")
  JOIN weather_winddir wd ON wt.id_city = wd.id_city AND DATE(wt."time") = DATE(wd."time")
  JOIN weather_cloud wc ON wt.id_city = wc.id_city AND DATE(wt."time") = DATE(wc."time")
  WHERE 
    wt."time" >= '{{ execution_date }}'::date
    AND wt."time" < '{{ execution_date + macros.timedelta(days=1) }}'::date
  GROUP BY DATE(wt."time"), dc.name_city
)
SELECT 
  date,
  name_city,
  min_temp_c,
  max_temp_c,
  avg_humidity,
  avg_wind_ms,  
  prevailing_wind_direction,
  prevailing_wind_direction_str,
  prevailing_cloudiness_percent,
  prevailing_cloudiness_type
FROM daily_weather
ORDER BY date, name_city;""",
    dag=dag)

task3 = PostgresOperator(
    task_id='dm_all',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT into weather_dm_alldays(date, name_city, min_temp_c, max_temp_c, avg_humidity, avg_wind_ms, prevailing_wind_direction, prevailing_wind_direction_str, prevailing_cloudiness_percent, prevailing_cloudiness_type)
select * from weather_dm_daily;""",
    dag=dag)

task1 >> task2 >> task3