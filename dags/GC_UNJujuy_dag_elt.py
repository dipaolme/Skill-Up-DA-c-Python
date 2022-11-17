from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
#from decouple import config
from assets.transform_dfs import transform_df
from pathlib import Path
import logging
import os


LOGS_DIR = '/usr/local/airflow/assets/'
LOGGER = logging.getLogger('GC_UNJujuy')
LOGGER.setLevel(logging.INFO)

FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')

HANDLER = logging.FileHandler(os.path.join(LOGS_DIR, 'GC_UNJujuy.log'))
HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(HANDLER)


def extract():
    sql_query = ""

    LOGGER.info("Comenzando proceso de ETL ...")
    LOGGER.info("Cargando query...")

    with open("/usr/local/airflow/include/GC_UNJujuy.sql", "r", encoding='utf8') as archivo:
        sql_query = archivo.read()
    
    LOGGER.info("Extrayendo datos...")

    hook = PostgresHook(postgres_conn_id='alkemy_db')
    conn = hook.get_conn()

    LOGGER.info("Guardando datos extraidos...")
    
    df = hook.get_pandas_df(sql=sql_query)

    df.to_csv('./files/GC_UNJujuy_select.csv', index=False)

    conn.close()


def transform():
    LOGGER.info("Iniciando proceso de tranformacion de datos...")

    df = pd.read_csv('./files/GC_UNJujuy_select.csv')

    LOGGER.info("Transformando datos...")

    df.rename(columns={'codigo_postal':'postal_code'}, inplace=True)
    df = transform_df(df)

    LOGGER.info("Guardando datos transformados...")

    df.to_csv('./datasets/GC_UNJujuy_process.txt', sep='\t', index=False)

    LOGGER.info("Enviando datos al bucket...")


with DAG(
    dag_id='GC_UNJujuy_dag_elt',
    description='Extrraccion, tranformacion y carga a s3 de datos de la Universidad Nacional de Jujuy.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 4),
    max_active_runs=5
) as dag:
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform
    )

    load_task = LocalFilesystemToS3Operator(
        task_id='load_to_s3_task',
        filename='/usr/local/airflow/datasets/GC_UNJujuy_process.txt',
        dest_key='GC_UNJujuy_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id="aws_s3_bucket",
        replace=True,
    )
    

    extract_task >> transform_task >> load_task