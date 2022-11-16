from datetime import timedelta,datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.models import DAG 
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from dateutil.relativedelta import relativedelta
import logging
import os 
import pandas as pd 
import csv 


WORKING_DIR = os.getcwd()
LOGS_DIR = os.path.join(WORKING_DIR, 'tests')  
logger = logging.getLogger('GIMoron')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')

handler = logging.FileHandler(os.path.join(LOGS_DIR, 'GIMoron.log'))
handler.setFormatter(formatter)

logger.addHandler(handler)


default_args = {
    "owner": "wmurillo",
    "depends_on_past":False,
    "email": ["murillowilmar1@hotmail.com"], 
    "email_on_failure": False,
    "email_on_retry": False,  
    "retries": 5, 
    "retry_delay":timedelta(minutes=1)
}


def extr_data():
   
   pg_hook = PostgresHook(postgres_conn_id="alkemy_db", schema ="training")
   #pg.conn=pg_hook.get_conn()
   #cursor = pg_conn.cursor()
   #cursor.execute(sql_stm)
   #return cursor.fetchall()
   with open ("/usr/local/airflow/include/GIUMoron.sql", "r") as sqlfile:

    sql_stm= sqlfile.read()
   df = pg_hook.get_pandas_df(sql = f"{sql_stm}") 
   df.to_csv("/usr/local/airflow/tests/GIUMoron.csv")


def transform():
  logging.info("transform")
   
  url= "/usr/local/airflow/tests/GIUMoron.csv"
  data_1=pd.read_csv(url)
# Eliminar variable cero 
  data_1 = data_1.drop(["Unnamed: 0" ], axis=1)
# renombrar variables 
  data_1 = data_1.rename(columns={"fechaiscripccion":"inscription_date", "nacimiento":"birth_day","universidad":"university", "carrerra":"career", "nombrre":"full_name", "sexo":"gender", "direccion":"location", "eemail":"email","codgoposstal":"postal_code"})
   
# aplicar tipo de variables 

  data_1["university"]=data_1["university"].apply(str)
  data_1["career"]=data_1["career"].apply(str)
  data_1["full_name"]=data_1["full_name"].apply(str)
  data_1["email"]=data_1["email"].apply(str)
  data_1["location"]=data_1["location"].apply(str)

# cambiar nombre a variable gender 
  data_1["gender"].replace(["F","M"], ["female","male" ], inplace=True)
  data_1["gender"]=data_1["gender"].apply(str)
#Aplicar normalización 

  data_1['university'] = data_1['university'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")
  data_1['career'] = data_1['career'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")
  data_1['location'] = data_1['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")
  data_1
#calcular edad 

  data_1['birth_day'] = pd.to_datetime(data_1['birth_day'], format='%Y-%m-%d')
  data_1['inscription_date'] = pd.to_datetime(data_1['inscription_date'], format='%Y-%m-%d')
  data_1= data_1.drop(data_1[data_1['inscription_date'] <= data_1['birth_day']].index)
  data_1= data_1.reset_index(drop=True)
  data_1['age'] = datetime.now() - data_1['birth_day']
  data_1['age'] = (data_1['age'].dt.days)/365
  data_1 = data_1.drop(data_1[data_1['age'] < 18].index)
  data_1 = data_1.reset_index(drop=True)
  data_1["age"]=data_1["age"].apply(int)
  data_1['inscription_date'] = data_1['inscription_date'].apply(str)
  data_1["birth_day"]=data_1["birth_day"].apply(str)
  data_1["postal_code"]=data_1["postal_code"].apply(str)



  # pasar archivo csv a txt 
  data_1.to_csv("/usr/local/airflow/tests/GIUMoron_process.txt")
  df = pd.read_csv("/usr/local/airflow/tests/GIUMoron_process.csv")
  content = str(df)
  print(content, file=open('/usr/local/airflow/tests/GIUMoron_process.txt', 'w'))
     
  

#def load_s3():
   
    #logging.info("save")



with DAG(

    "GI_moron_pampas", 
    default_args=default_args,
    start_date= datetime(2022, 8,11),
    max_active_runs =5, 
    description = " DAG_pampa",
    schedule_interval="@hourly", 
    tags=["DAG_moron_pampas"], 
    catchup =False, 
    template_searchpath = "/usr/local/air_flow/include/"
) as dag: 
   
    extract_data= PythonOperator(task_id="extract_data", python_callable=extr_data)
    transform_data = PythonOperator(task_id="transform_data", python_callable=transform)
    load_data = LocalFilesystemToS3Operator(
        task_id = "load_data",
        filename='/usr/local/airflow/tests/GIUMoron_process.txt',
        dest_key='GIUMoron_process.txt',
        dest_bucket='awswmurillo',
        aws_conn_id="aws_s3_bucket2",
        replace=True
    )

    load_data2 = LocalFilesystemToS3Operator(
        task_id = "load_data2",
        filename='/usr/local/airflow/tests/GIUMoron_process.txt',
        dest_key='GIUJujuy_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id="aws_s3_bucket",
        replace=True
    )

    extract_data >> transform_data >> load_data  >> load_data2
