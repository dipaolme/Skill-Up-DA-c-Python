from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
#from decouple import config
from assets.transform_dfs import transform_df
from pathlib import Path

def extract():
    sql_query = ""

    with open("/usr/local/airflow/include/GC_UNJujuy.sql", "r", encoding='utf8') as archivo:
        sql_query = archivo.read()

    hook = PostgresHook(postgres_conn_id='alkemy_db')

    conn = hook.get_conn()
    
    df = hook.get_pandas_df(sql=sql_query)

    df.to_csv('./files/GC_UNJujuy_select.csv', index=False)

    conn.close()


def transform():
    df = pd.read_csv('./files/GC_UNJujuy_select.csv')

    df.rename(columns={'codigo_postal':'postal_code'}, inplace=True)
    df = transform_df(df)

    df.to_csv('./datasets/GC_UNJujuy_process.txt', sep='\t', index=False)


with DAG(
    dag_id='GC_UNJujuy_dag_elt',
    description='Este es un tutorial de dags',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 4)
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