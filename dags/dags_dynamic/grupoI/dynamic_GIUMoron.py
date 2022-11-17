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



default_args = {
    "owner": "nameofdeveloper",
    "depends_on_past":False,
    "email": ["email"], 
    "email_on_failure": False,
    "email_on_retry": False,  
    "retries": 5, 
    "retry_delay":timedelta(minutes=1)
}


def extr_data():
    
   pg_hook = PostgresHook(postgres_conn_id="alkemy_db", schema ="training")
   
   with open ("/usr/local/airflow/include/", "r") as sqlfile:  #completar la ruta  con sql 

    sql_stm= sqlfile.read()
   df = pg_hook.get_pandas_df(sql = f"{sql_stm}") 
   df.to_csv("/usr/local/airflow/tests/")   # completar la ruta con csv





def transform():

   
   url= "/usr/local/airflow/tests/"   # completar la ruta 
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


#Aplicar normalizaci�n a universidad y carrera 
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

   

   
# Convertir csv a txt 
   data_2.to_csv("/usr/local/airflow/tests/")    #complete the path csv
   df = pd.read_csv("/usr/local/airflow/tests/")  #complete the path csv
   content = str(df)
   print(content, file=open('/usr/local/airflow/tests/', 'w')) #complete the path txt


   




with DAG(dag_id = "dynamic_GIUMoron",
      start_date= datetime(2022, 8,11),
      default_args=default_args,
      max_active_runs =5, 
      description = " descripcion del dag ",
      schedule_interval = "@hourly",   # tiempo de ejecuci�n 
      tags=["Etiquetar el Dag"], 
      catchup = False
      
) as dag: 
   
   
   #Task 1 
    extract_data= PythonOperator(task_id="extract_data", python_callable=extr_data)

   #Task 2
    transform_data = PythonOperator(task_id="transform_data", python_callable=transform)

    #load_data = PythonOperator(task_id="load_data", python_callable=load_s3)  # posible operador futuro 

   #Task 3 
    load_data = LocalFilesystemToS3Operator(
       task_id = "lnombre de la tarea ",
        filename='/usr/local/airflow/tests/',# ruta del archivo txt o archivo a subir 
        dest_key='GIUJujuy_process.txt',  # se pone el tipo de archivo que se quiere generar en aws 
        dest_bucket='usuario de la conexi�n',
        aws_conn_id="conexion aws ",
        replace=True
    )
    
    #Task 4 
    load_data2 = LocalFilesystemToS3Operator(
        task_id = "lnombre de la tarea ",
        filename='/usr/local/airflow/tests/',# ruta del archiv txt o archivo a subir 
        dest_key='GIUJujuy_process.txt',  # se pone el tipo de archivo que se quiere generar en aws 
        dest_bucket='usuario de la conexi�n',
        aws_conn_id="conexion aws ",
        replace=True
    )

    extract_data >> transform_data >> load_data>> load_data2