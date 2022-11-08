
# BROC95
from plugins.sqlCommandB import csvFile, identExt
from plugins.connectionDag import configDag, configLog
from plugins.dataTrasB import dataTransf

from datetime import datetime, timedelta
from plugins.sqlCommandB import sqlCommand, createPath
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from sqlalchemy import create_engine
import logging.config
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging



name_data = 'GBUNSalvador'


dag_name = '_dag_elt'
selec = "_select.csv"
sql_ = ".sql"

dag_ = name_data+dag_name
query_name = name_data+sql_
select_name = name_data+selec

default_args, POSTGRES_CONN_ID = configDag()




#  Extract data with  hook,pandas .csv
def extract():
  
    logger = configLog(dag_)

    logger.info(dag_)
    logger.info("Extract")
    logger.info("Connect: % s", POSTGRES_CONN_ID)

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    query = sqlCommand(
        file=query_name, point='include')
    conn = hook.get_conn()
    # logging.info(conn)
    
    

    df = hook.get_pandas_df(sql=query)

    logger.info(df.head())
    pathCsv = createPath('files')
    # pathCsv = createPath('include')  # Correccion a guardar localmente

    js = df.to_json(orient='columns')
    # print("Create csv")
    df.to_csv(pathCsv+'/'+select_name)
    conn.close()
    


#  Transform data with pandas
def transform():
    logger = configLog(dag_)
    logger.info("Transform")

    pathfile = createPath('files')

    fileSelect = csvFile(pathfile, select_name)
    dataTransf(fileSelect)

    # print(POSTGRES_CONN_ID)


#  Load data with S3 amazon .txt
def load(some_parameter):
    logger = configLog(dag_)
    logger.info("Load: %s", dag_)
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
# task1 
