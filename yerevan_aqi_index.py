import requests
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable


default_args = {
    'owner': "S_D",
    'retries': 0
}

with DAG(
    "yerevan_aqi_index",
    default_args=default_args,
    description="Yerevan AQI Index",
    schedule='*/5 * * * *',
    start_date=datetime(2026, 5, 26,0, 0),
    tags=['test'],
    catchup=False,
) as dag:

    def get_aqi_index_func():

        api_key = Variable.get("purpleair_api_key")
        url = f"https://api.purpleair.com/v1/sensors/252917?fields=humidity,temperature,pm2.5,pm2.5_10minute&api_key={api_key}"
        response = requests.get(url)

        #api_key taken from: https://develop.purpleair.com/

        if response.status_code == 200:

            value = response.json()
            sensor = value["sensor"]

            def f_into_c(f):
                c = round((f - 32) * 5/9, 1)
                return c

            data = {
                "status": "success",
                "date_time": sensor["stats"]["time_stamp"],
                "humidity":  sensor["humidity"],
                "temperature":  f_into_c(sensor["temperature"]),
                "pm2_5": sensor["pm2.5"]
            }

            return data

        else:

            data = {
                "status": "error",
                "error_code": response.status_code
            }

            return data

    def insert_data_func(ti):

        pg_hook = PostgresHook(postgres_conn_id="pg")

        extracted_data = ti.xcom_pull(task_ids="get_aqi_index")

        if extracted_data["status"] == "success":

            sql = """INSERT INTO yerevan_aqi_index (date_time, humidity, temperature, pm2_5)
                VALUES (to_timestamp(%s), %s, %s, %s)
                ON CONFLICT (date_time) DO NOTHING"""

            parameters = (

                 extracted_data["date_time"],
                 extracted_data["humidity"],
                 extracted_data["temperature"],
                 extracted_data["pm2_5"]

            )

            pg_hook.run(sql, parameters=parameters)

        else:

            sql = """INSERT INTO yerevan_aqi_index_errors (error)
                VALUES (%s)"""

            parameters = (
                extracted_data["error_code"],
            )

            pg_hook.run(sql, parameters=parameters)


    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="pg",
        sql="""
                CREATE TABLE IF NOT EXISTS yerevan_aqi_index (
                    date_time TIMESTAMPTZ(0) PRIMARY KEY,
                    humidity INTEGER,
                    pm2_5 NUMERIC,
                    temperature NUMERIC
                );
                
                CREATE TABLE IF NOT EXISTS yerevan_aqi_index_errors (
                    date_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    error INTEGER
                )
                
            """
    )

    get_aqi_index = PythonOperator(
        task_id="get_aqi_index",
        python_callable=get_aqi_index_func
    )

    insert_data = PythonOperator(
        task_id="insert_data",
        python_callable=insert_data_func
    )

create_table >> get_aqi_index >> insert_data





