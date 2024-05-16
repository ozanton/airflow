from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from utils.check_table_sensor import CheckTableSensor

connection = BaseHook.get_connection("weather_postgreSQL_con")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 2),
    "retries": 3,
    "retry_delay": timedelta(seconds=10),
    #"trigger_rule": "all_success",  # правило выполнения
}

dag = DAG('dag_dm_windy', default_args=default_args, schedule_interval='10 1/3 * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["data mart+sensor", "fifth"])

# Sensor
wait_for_dag_run_windy = ExternalTaskSensor(
    task_id='wait_for_dag_run_windy',
    external_dag_id='dag_run_windy',
    external_task_id=None,
    mode='reschedule',
    poke_interval=10,
    dag=dag,
)

task1 = PostgresOperator(
    task_id='clear_wind',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM windy_wind_daily""",
    dag=dag)

task2 = PostgresOperator(
    task_id='clear_other',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""DELETE FROM windy_other_daily""",
    dag=dag)

task3 = PostgresOperator(
    task_id='windy_wind_daily',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_wind_daily (name_city, date, min_wind_speed, max_wind_speed, wind_dir)
WITH WindData AS (
    SELECT 
        dc.name_city,
        DATE_TRUNC('day', ww."timestamp") AS date,
        CAST(CASE WHEN ww.wind_u = 0.0 THEN 0.00001
                WHEN ww.wind_u = -0.0 THEN -0.00001
                ELSE ww.wind_u
            END AS DECIMAL(10, 1)
        ) AS wind_u,
        CAST(CASE WHEN ww.wind_v = 0.0 THEN 0.00001
                WHEN ww.wind_v = -0.0 THEN -0.00001
                ELSE ww.wind_v
            END AS DECIMAL(10, 1)
        ) AS wind_v,
        CAST(SQRT(POW(CASE WHEN ww.wind_u = 0.0 THEN 0.00001
                        WHEN ww.wind_u = -0.0 THEN -0.00001
                        ELSE ww.wind_u
                    END::DOUBLE PRECISION, 2
                ) + POW(CASE WHEN ww.wind_v = 0.0 THEN 0.00001
                        WHEN ww.wind_v = -0.0 THEN -0.00001
                        ELSE ww.wind_v
                    END::DOUBLE PRECISION, 2
                )
            ) AS DECIMAL(10, 1)
        ) AS wind_speed,
        CASE 
            WHEN EXTRACT(HOUR FROM ww."timestamp") = 12 THEN 90 - CAST(ATAN(ww.wind_v / ww.wind_u) / PI() * 180 AS INTEGER)
            ELSE NULL
        END AS wind_dir
    FROM 
        windy_wind ww
    JOIN 
        dict_cities dc ON ww.id_city = dc.id_city
)
SELECT 
    name_city,
    date,
    MIN(wind_speed) AS min_wind_speed,
    MAX(wind_speed) AS max_wind_speed,
    (SELECT DISTINCT wind_dir FROM WindData wd WHERE wd.name_city = WindData.name_city AND wd.date = WindData.date AND wind_dir IS NOT NULL LIMIT 1) AS wind_dir
FROM 
    WindData
WHERE 
    date >= '{{ execution_date }}'::date
    AND date  < '{{ execution_date + macros.timedelta(days=1) }}'::date
GROUP BY 
    name_city, date
ON CONFLICT (name_city, date) DO UPDATE
SET min_wind_speed = EXCLUDED.min_wind_speed, 
    max_wind_speed = EXCLUDED.max_wind_speed, 
    wind_dir = EXCLUDED.wind_dir;""",
    dag=dag)

task4 = PostgresOperator(
    task_id='windy_other_daily',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT into windy_other_daily (name_city, date, total_precip, avg_pressure, prevalier_ptype, avg_clouds)
select 
	dc.name_city,
    DATE(wp."timestamp") AS date,
    SUM(wp.past3hprecip) AS total_precip,
	CAST(AVG(pw.pressure) AS numeric(10, 1)) AS avg_pressure,
    max(pt.ptype) as prevalier_ptype,
    CAST(AVG((lclouds + mclouds + hclouds)) AS NUMERIC(5, 1)) AS avg_clouds
from windy_precip wp 
join dict_cities dc on wp.id_city = dc.id_city 
    left join windy_pressure pw on wp.id_city = pw.id_city and DATE(wp."timestamp") = DATE(pw."timestamp")
    left join windy_ptype pt on wp.id_city = pt.id_city and DATE(wp."timestamp") = DATE(pt."timestamp")
    left join windy_clouds wc on wp.id_city = wc.id_city and DATE(wp."timestamp") = DATE(wc."timestamp")
WHERE 
    wp."timestamp" >= '{{ execution_date }}'::date AND wp."timestamp" < '{{ execution_date + macros.timedelta(days=1) }}'::date
GROUP BY dc.name_city, DATE(wp."timestamp")
order by DATE(wp."timestamp") desc
ON CONFLICT (name_city, date) DO UPDATE
SET total_precip = EXCLUDED.total_precip, 
    avg_pressure = EXCLUDED.avg_pressure, 
    prevalier_ptype = EXCLUDED.prevalier_ptype,
    avg_clouds = EXCLUDED.avg_clouds;""",
    dag=dag)

task_check_windy_wind_daily = CheckTableSensor(
    task_id=f'task_check_windy_wind_daily',
    timeout=1000,
    mode='reschedule',
    poke_interval=10,
    conn=connection,
    table_name='windy_wind_daily',
    dag=dag
)

task_check_windy_other_daily = CheckTableSensor(
    task_id=f'task_check_windy_other_daily',
    timeout=1000,
    mode='reschedule',
    poke_interval=10,
    conn=connection,
    table_name='windy_other_daily',
    dag=dag
)

task5 = PostgresOperator(
    task_id='windy_dm_all',
    postgres_conn_id="weather_postgreSQL_con",
    sql="""INSERT INTO windy_dm_alldays (name_city, date, min_temp_c, max_temp_c, min_dewpoint, max_dewpoint, min_wind_speed, max_wind_speed, wind_dir, max_gust, avg_relative_humidity, total_precip, avg_pressure, prevalier_ptype, avg_clouds)
SELECT 
    dc.name_city,
    DATE(wt."timestamp") AS date,
    MIN(ROUND(CAST(temp - 273.15 AS NUMERIC), 1)) AS min_temp_c,
    MAX(ROUND(CAST(temp - 273.15 AS NUMERIC), 1)) AS max_temp_c,
    MIN(ROUND(CAST(dewpoint - 273.15 AS NUMERIC), 1)) AS min_dewpoint,
    MAX(ROUND(CAST(dewpoint - 273.15 AS NUMERIC), 1)) AS max_dewpoint,
    wwd.min_wind_speed,
    wwd.max_wind_speed,
    wwd.wind_dir,
    CAST(MAX(gust) AS numeric(5, 1)) AS max_gust,
    CAST(AVG(wr.rh) AS INTEGER) AS avg_relative_humidity,
    wod.total_precip,
    wod.avg_pressure,
    wod.prevalier_ptype,
    wod.avg_clouds
FROM windy_temp wt
JOIN dict_cities dc ON wt.id_city = dc.id_city
    left join windy_dewpoint wd on wt.id_city = wd.id_city and DATE(wt."timestamp") = DATE(wd."timestamp")
    left join windy_rh wr on wt.id_city = wr.id_city and DATE(wt."timestamp") = DATE(wr."timestamp")
    join windy_wind_daily wwd on wwd.name_city = dc.name_city
    left join windy_gust wg on wt.id_city = wg.id_city and DATE(wt."timestamp") = DATE(wg."timestamp")
    join windy_other_daily wod on wod.name_city = dc.name_city
WHERE 
    wt."timestamp" >= '{{ execution_date }}'::date AND wt."timestamp" < '{{ execution_date + macros.timedelta(days=1) }}'::date
GROUP BY dc.name_city, DATE(wt."timestamp"), wwd.min_wind_speed, wwd.max_wind_speed, wwd.wind_dir, wod.total_precip,
    wod.avg_pressure, wod.prevalier_ptype, wod.avg_clouds
order by DATE(wt."timestamp") desc
ON CONFLICT (name_city, date) DO UPDATE
SET min_temp_c = EXCLUDED.min_temp_c, 
    max_temp_c = EXCLUDED.max_temp_c, 
    min_dewpoint = EXCLUDED.min_dewpoint,
    max_dewpoint = EXCLUDED.max_dewpoint,
    min_wind_speed = EXCLUDED.min_wind_speed,
    max_wind_speed = EXCLUDED.max_wind_speed,
    wind_dir = EXCLUDED.wind_dir,
    max_gust = EXCLUDED.max_gust,
    avg_relative_humidity = EXCLUDED.avg_relative_humidity,
    avg_pressure = EXCLUDED.avg_pressure,
    prevalier_ptype = EXCLUDED.prevalier_ptype,
    avg_clouds = EXCLUDED.avg_clouds;""",
    dag=dag)

wait_for_dag_run_windy.set_downstream([task1, task2])
task1.set_downstream(task3)
task2.set_downstream(task4)
task3.set_downstream([task_check_windy_wind_daily])
task4.set_downstream([task_check_windy_other_daily])
task5.set_upstream([task_check_windy_wind_daily, task_check_windy_other_daily])