#import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd

#defino un diccionario con las variables del DAG
default_args = {
    #'schedule_interval' : "@hourly",   NO VA ACA
    'start_date' : datetime(2022, 11, 4),
    'catchup' : False,
    'retries': 5,
    'owner' : 'alfredo'
    }

#path_sql = '/usr/local/airflow/include/GE_UniInteramericana.sql',
#path_csv = '/usr/local/airflow/tests/GE_LaPampa_select.csv'
# se pueden definir también con op_args = [arg1, arg2]

# defino la funcion de extraccion y generacion de los csv
def extract_csv():
    with open (f'/usr/local/airflow/include/GE_UniInteramericana.sql', 'r') as sqfile:
        query = sqfile.read() 
    hook = PostgresHook(postgres_conn_id="alkemy_db")
    #logging.info("Exporting query to file")
    df = hook.get_pandas_df(sql=query)
    df.to_csv('/usr/local/airflow/tests/GE_Interamericana_select.csv')

# se define el DAG
with DAG(
    dag_id = "DAG_Uni_Interamericana",
    default_args = default_args,
    schedule_interval="@hourly",
    tags = ['ETL Universidad Interamericana']
) as dag:

# se definen los tasks
    tarea_1 = PythonOperator(
        task_id = "extract",
        python_callable = extract_csv   # para ejecutar una función se llama con python_callable
        #op_args = [path_sql, path_csv]
    
    )

# se definen las dependencias de las tareas
    tarea_1 # >> tarea_2 >> tarea_3