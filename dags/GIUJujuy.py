from datetime import timedelta,datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from dateutil.relativedelta import relativedelta
import logging
import os 
import pandas as pd 


# logger 
WORKING_DIR = os.getcwd()
LOGS_DIR = os.path.join(WORKING_DIR, 'tests')  
logger = logging.getLogger('GIJujuy')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')
handler = logging.FileHandler(os.path.join(LOGS_DIR, 'GIJujuy.log'))
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
   #  logging.info("scrapping")
   pg_hook = PostgresHook(postgres_conn_id="alkemy_db", schema ="training")
   #pg.conn=pg_hook.get_conn()
   #cursor = pg_conn.cursor()
   #cursor.execute(sql_stm)
   #return cursor.fetchall()
   with open ("/usr/local/airflow/include/GIUJujuy.sql", "r") as sqlfile:

    sql_stm= sqlfile.read()
   df = pg_hook.get_pandas_df(sql = f"{sql_stm}") 
   df.to_csv("/usr/local/airflow/tests/GIUJujuy.csv")





def transform():

   # logging.info("processing")
   url= "/usr/local/airflow/tests/GIUJujuy.csv"
   data_2=pd.read_csv(url)

# Eliminar variable cero 
   data_2 = data_2.drop (["Unnamed: 0" ], axis=1)
# renombrar variables 
   data_2 = data_2.rename(columns={"nombre":"full_name", "sexo":"gender", "birth_date":"birth_day"})
   

# aplicar tipo de variables str
   data_2["university"]=data_2["university"].apply(str)
   data_2["career"]=data_2["career"].apply(str)
   data_2["full_name"]=data_2["full_name"].apply(str)
   data_2["email"]=data_2["email"].apply(str)
   data_2["location"]=data_2["location"].apply(str)


#Aplicar normalización a universidad y carrera 
   data_2['university'] = data_2['university'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")
   data_2['career'] = data_2['career'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")
   data_2['location'] = data_2['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")

 #cambiar nombre a variable gender y reemplazando variables 
   data_2["gender"].replace(["f","m"], ["female","male" ], inplace=True)
   data_2["gender"]=data_2["gender"].apply(str)
   

#calcular edad 
   data_2['birth_day'] = pd.to_datetime(data_2['birth_day'], format='%Y-%m-%d')
   data_2['inscription_date'] = pd.to_datetime(data_2['inscription_date'], format='%Y-%m-%d')
   data_2= data_2.drop(data_2[data_2['inscription_date'] <= data_2['birth_day']].index)
   data_2= data_2.reset_index(drop=True)
   data_2['age'] = datetime.now() - data_2['birth_day']
   data_2['age'] = (data_2['age'].dt.days)/365
   data_2 = data_2.drop(data_2[data_2['age'] < 18].index)
   data_2 = data_2.reset_index(drop=True)
   data_2["age"]=data_2["age"].apply(int)
   data_2['inscription_date'] = data_2['inscription_date'].apply(str)
   data_2["birth_day"]=data_2["birth_day"].apply(str)

   

# Manipular tabla codigos postales 
   cod_ps = "/usr/local/airflow/tests/codigos_postales.csv"
   data_pos= pd.read_csv(cod_ps)
   data_pos['localidad'] = data_pos['localidad'].astype(str)
   data_pos['localidad'] = data_pos['localidad'].str.lower().str.replace("-", " ").str.replace("  ", " ")
   data_pos = data_pos.drop_duplicates(['localidad'], keep='last')
   data_in = pd.merge(left=data_2,right=data_pos, left_on='location', right_on='localidad')
   data_in= data_in.drop(['postal_code', 'localidad'], axis=1)
   data_2=data_in

   data_2 = data_2.rename(columns={"codigo_postal":"postal_code"})
   
   
# Convertir csv a txt 
   data_2.to_csv("/usr/local/airflow/tests/GIUJujuy_process.txt")
   df = pd.read_csv("/usr/local/airflow/tests/GIUJujuy_process.csv")
   content = str(df)
   print(content, file=open('/usr/local/airflow/tests/GIUJujuy_process.txt', 'w'))


with DAG(

    "GI_jujuy_utn", 
    default_args=default_args,
    start_date= datetime(2022, 8,11),
    max_active_runs =5, 
    description = " DAG_jujuy",
    schedule_interval="@hourly", 
    tags=["DAG_jujuy_utn"], 
    catchup =False, 
    template_searchpath = "/usr/local/air_flow/include/"
) as dag: 
   
   
   #Task 1 
    extract_data= PythonOperator(task_id="extract_data", python_callable=extr_data)

   #Task 2
    transform_data = PythonOperator(task_id="transform_data", python_callable=transform)

    #load_data = PythonOperator(task_id="load_data", python_callable=load_s3)

   #Task 3 
    load_data = LocalFilesystemToS3Operator(
        task_id = "load_data",
        filename='/usr/local/airflow/tests/GIUJujuy_process.txt',
        dest_key='GIUJujuy_process.txt',
        dest_bucket='awswmurillo',
        aws_conn_id="aws_s3_bucket2",
        replace=True
    )

    load_data2 = LocalFilesystemToS3Operator(
      task_id = "load_data2",
        filename='/usr/local/airflow/tests/GIUJujuy_process.txt',
        dest_key='GIUJujuy_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id="aws_s3_bucket",
        replace=True
    )

    extract_data >> transform_data >> load_data >> load_data2
