
# BROC95
from plugins.sqlCommandB import csvFile
from plugins.connectionDag import configDag
from plugins.dataTrasB import dataTransf

from datetime import datetime, timedelta
from plugins.sqlCommandB import sqlCommand, createPath
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging


import os
from airflow.providers.postgres.hooks.postgres import PostgresHook


logging.info("Def var dag")
default_args, POSTGRES_CONN_ID = configDag()


name_data = 'GBUNComahue'


dag_name = '_dag_elt'
selec = "_select.csv"
sql_ = ".sql"

dag_ = name_data+dag_name
query_name = name_data+sql_
select_name = name_data+selec


#  Extract data with  hook,pandas .csv
def extract():
    logging.info("Connect: %s", POSTGRES_CONN_ID)
    logging.info("Extract: %s", dag_)
    hook = PostgresHook(postgress_conn_id=POSTGRES_CONN_ID)
    query = sqlCommand(
        file=query_name, point='include')

    # logging.info(os.getcwd())
    df = hook.get_pandas_df(sql=query)

    # logging.info('query')

    logging.info(df.head())

    pathCsv = createPath('files')

    # jsondat = json.load(df)
    js = df.to_json(orient='columns')
    # print("Create csv")
    df.to_csv(pathCsv+'/'+select_name)

    # print(os.listdir(pathCsv))


#  Transform data with pandas
def transform():
    logging.info("Transform: %s", dag_)

    pathfile = createPath('files')

    fileSelect = csvFile(pathfile, select_name)
    dataTransf(fileSelect)

    # print(POSTGRES_CONN_ID)


#  Load data with S3 amazon .txt
def load(some_parameter):
    logging.info("Load: %s", dag_)
    pass


with DAG(dag_id=dag_, start_date=datetime(2022, 11, 4), schedule_interval=timedelta(hours=1), default_args=default_args, catchup=False) as dag:
    #  Extract
    task1 = PythonOperator(task_id="TaskExtract",
                           python_callable=extract)
    #  Transform
    task2 = PythonOperator(task_id="TaskTransform",
                           python_callable=transform)
    #  Load

task1 >> task2