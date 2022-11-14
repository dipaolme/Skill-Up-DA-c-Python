import logging
import os
from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np


##################### Connection #####################

POSTGRES_CONN_ID = "alkemy_db"
AWS_CONN_ID = "aws_s3_bucket" 

##################### Logging Config #################

LOGS_DIR = '/usr/local/airflow/tests/'
LOGGER = logging.getLogger('GAUFlores')
LOGGER.setLevel(logging.INFO)

FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')

HANDLER = logging.FileHandler(os.path.join(LOGS_DIR, 'GAUFlores.log'))
HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(HANDLER)

######################### DAG ########################

def pg_extract2csv():

  LOGGER.info("Starting dag ETL process...")
  LOGGER.info("Loading data...")

  pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
  
  with open('/usr/local/airflow/include/GAUFlores.sql','r') as sqlfile:
    sql_stm= sqlfile.read()
  
  df = pg_hook.get_pandas_df(sql = f'{sql_stm}')
  
  df.to_csv('/usr/local/airflow/tests/GAUFlores_select.csv')
  
  LOGGER.info("Data successfully loaded")

def pd_transform2txt():

  LOGGER.info("Transforming data")
  
  df = pd.read_csv('/usr/local/airflow/tests/GAUFlores_select.csv', index_col=0)
  
  df['university'] = df['university'].str.lower().str.strip()
  df['career'] = df['career'].str.lower().str.strip()
  df['last_name'] = df['last_name'].str.lower().str.strip().str.replace('(m[r|s]|[.])|(\smd\s)', '', regex=True)
  df['email'] = df['email'].str.lower().str.strip()
  df['gender'] = df['gender'].map({'F': 'female', 'M': 'male'})
  df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%Y/%m/%d')
  df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'])

  today = datetime.now()
  df['age'] = np.floor((today - df['fecha_nacimiento']).dt.days / 365)
  df['age'] = df['age'].apply(lambda x: x if (x > 18.0) and (x < 80) else -1)
  df['age'] = np.where(df['age']== -1, 21, df['age'])
  df['age'] = df['age'].astype(int)

  df = df.drop(columns='fecha_nacimiento')
 
  cp_loc_df = pd.read_csv('/usr/local/airflow/tests/codigos_postales.csv')
  cp_loc_df = cp_loc_df.rename(columns={'codigo_postal':'postal_code', 'localidad':'location'})
  cp_loc_df['location'] = cp_loc_df['location'].str.lower().str.strip().str.replace('_',' ')
  
  if 'location' in df.columns.to_list():
    cp_loc_df['location'] = cp_loc_df['location'].drop_duplicates(keep='first')
    df = df.merge(cp_loc_df, how='left', on='location')
  else:
    df = df.merge(cp_loc_df, how='left', on='postal_code')


  df = df[['university', 'career', 'inscription_date', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]

  df.to_csv('/usr/local/airflow/tests/GAUFlores_process.txt', sep='\t', index=False)

  LOGGER.info("Data succesfully transformed")
  LOGGER.info("Loading data to s3 bucket...")


with DAG(
    "GAUFlores_ETL",
    start_date=datetime(2022, 3, 11),
    schedule_interval="@hourly",
    default_args={
        "retries": 5
    },
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_task",
        python_callable=pg_extract2csv
    )

    transform = PythonOperator(
        task_id="transform_task",
        python_callable = pd_transform2txt
    )

    load = LocalFilesystemToS3Operator(
        task_id='load_to_s3_task',
        filename='/usr/local/airflow/tests/GAUFlores_process.txt',
        dest_key='GAUFlores_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id=AWS_CONN_ID,
        replace=True,
    )
    
    extract >> transform >> load