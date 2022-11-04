#import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd


POSTGRES_CONN_ID = "alkemy_db"

def pg_extract2csv():
  pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
  #logging.info("Exporting query to file")
  with open('/usr/local/airflow/include/GAUFlores.sql','r') as sqlfile:
    sql_stm= sqlfile.read()
  df = pg_hook.get_pandas_df(sql = f'{sql_stm}')
  df.to_csv('/usr/local/airflow/tests/GAUflores_select.csv')

# Instantiate DAG
with DAG(
    "GAUflores_ETL",
    start_date=datetime(2022, 3, 11),
    max_active_runs=3,
    schedule_interval="@daily",
    #default_args={
    #    "email_on_failure": False,
    #    "email_on_retry": False,
    #    "retries": 1,
    #    "retry_delay": timedelta(minutes=1),
    #},
    catchup=False,
    template_searchpath="/usr/local/airflow/include/"
) as dag:
    extract = PythonOperator(
        task_id="extract_task",
        python_callable=pg_extract2csv
        #op_args = ['GauFlores.sql']  probar pasarlo como para metro

    )

    # transform = PythonOperator(
    #     task_id="transform_task",
    #     python_callable = 'funcion que haga cosas con pndas y los csv'
    # )

    # load = 'OperadorS3'(
    #     task_id='load_task'
    # )


 
    extract # >> transform >> load