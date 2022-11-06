#import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
#from operators.s3_to_postgres_operator import S3ToPostgresOperator
#from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd

#defino un diccionario con las variables del DAG
default_args = {
    #'schedule_interval' : "@hourly",  NO VA ACA
    'start_date' : datetime(2022, 11, 4),
    'catchup' : False,
    'retries': 5,
    'owner' : 'alfredo'
    }

#path_sql = '/usr/local/airflow/include/GE_UniLaPampa.sql',
#path_csv = '/usr/local/airflow/tests/GA_LaPampa_select.csv'
# se pueden definir también con op_args = [arg1, arg2]

# defino la funcion de extraccion y generacion de los csv
def extract_csv():
    with open (f'/usr/local/airflow/include/GE_UniLaPampa.sql', 'r') as sqfile:
        query = sqfile.read() 
    hook = PostgresHook(postgres_conn_id="alkemy_db")
    #logging.info("Exporting query to file")
    df = hook.get_pandas_df(sql=query)
    df.to_csv('/usr/local/airflow/tests/GE_LaPampa_select.csv')

# se define el DAG
with DAG(
    dag_id = "DAG_Uni_LaPampa",
    default_args = default_args,
    schedule_interval="@hourly",
    tags = ['ETL Universidad La Pampa']
) as dag:

# se definen los tasks
    tarea_1 = PythonOperator(
        task_id = "extract",
        python_callable = extract_csv   # para ejecutar una función se llama con python_callable
        #op_args = [path_sql, path_csv]
    
    )

    #tarea_2 = PythonOperator(
    #    task_id = "transform",
    #)

    #tarea_3 = S3ToPostgresOperator(
    #    task_id = "load",
    #)

# se definen las dependencias de las tareas
    tarea_1 # >> tarea_2 >> tarea_3