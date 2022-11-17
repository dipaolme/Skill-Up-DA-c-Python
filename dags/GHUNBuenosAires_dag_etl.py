
#  Documentar operadores que se utilizan teniendo en cuenta:
#  1.Consultas SQL
#  2.Procesar datos con pandas
#  3.Cargar los datos en S3

from airflow import DAG

#  Importar librerías neceasarias
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from airflow.decorators import task

#  Operadores bash
from airflow.operators.empty import EmptyOperator

#  Operadores python
from airflow.operators.python import PythonOperator

#  Operadores postgres
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

#  Operadores S3
from airflow.providers.amazon.aws.transfers.local_to_s3 import (LocalFilesystemToS3Operator)







#  DAG Args Default
default_args = {  
                'owner': 'andres_sanchez',
                  'retries': 5,  
                  'retry_delay': timedelta(minutes=5)}

##Funciones para plugins

def extract_data():
    with open(f'/usr/local/airflow/include/GHUNBuenosAires.sql', 'r') as query:
        sqll = query.read()
    hook = PostgresHook (postgres_conn_id="alkemy_db")
    df = hook.get_pandas_df(sql=sqll)
    print(df.head())
    try:
        df.to_csv('/usr/local/airflow/files/GHUNBuenosAires_select.csv')
    except:
        print("Archivo existente")


def procesamiento_pandas():
    #Leer csv y postal_codes
    df_csv = pd.read_csv('/usr/local/airflow/files/GHUNBuenosAires_select.csv')
    df_pc = pd.read_csv('/usr/local/airflow/assets/codigos_postales.csv')
    
    #cambiar nombre a ID:
    df_csv = df_csv.rename(columns = {'Unnamed: 0': 'ID'})
    #Procesar Columnas 
    df_csv['university'] = df_csv['university'].str.lower().str.replace("-", " ").str.replace("  ", " ")
    
    df_csv['career'] = df_csv['career'].str.lower().str.replace("-", " ").str.replace("  ", " ")
    
    df_csv['inscription_date'] = pd.to_datetime(df_csv['inscription_date'], format='%Y-%m-%d')
    
    
    today = datetime.now()
    year= today.year
    #print("la fecha actual es:", today)
    #print("El año actual es:", year)
    
    df_csv['fechas_nacimiento'] = pd.to_datetime(df_csv['fechas_nacimiento'], format='%y-%b-%d')
    do = pd.tseries.offsets.DateOffset(years= 100)
    
    df_csv['fechas_nacimiento'] = ((df_csv['fechas_nacimiento']) - do).where(((df_csv['fechas_nacimiento']).dt.year > 2022), df_csv['fechas_nacimiento'])
    print(df_csv['fechas_nacimiento'])
    
    #Calculando los años:
    df_csv['age'] = (today.year - (df_csv['fechas_nacimiento']).dt.year) -((today.month) <(df_csv['fechas_nacimiento'].dt.month)) -(((today.month) == (df_csv['fechas_nacimiento'].dt.month)) & (today.day<df_csv['fechas_nacimiento'].dt.day))
    print(df_csv['age'])
    
    #Eliminando edades incoherentes, se eliminan registros con edades menor a 15 o mayor a 75
    df_csv = df_csv.drop(df_csv[df_csv['age'] < 15].index)
    df_csv = df_csv.drop(df_csv[df_csv['age'] > 75].index)
    
    #quitando mr. and ms.
    df_csv['last_name']=df_csv['last_name'].str.lower().replace("ms.-", "").replace("mr.-", "").str.replace("-", " ").str.replace("  ", " ")
    
    #Separando nombres y apellidos
    new=df_csv['last_name'].str.split(" ", 1, expand=True)
    df_csv['first_name']=new[0]
    df_csv['last_name']=new[1]
    
    #Géneros
    df_csv['gender'].iloc[df_csv['gender'] == 'm'] = 'male'
    df_csv['gender'].iloc[df_csv['gender'] == 'f'] = 'female'
    
    #Eliminando columnas innecesarias
    col_drop=['fechas_nacimiento', 'location']
    df_csv.drop(columns=col_drop, inplace=True)
    
    #archivo de códigos postales
    df_pc['localidad'] = df_pc['localidad'].astype(str)
    df_pc['localidad'] = df_pc['localidad'].str.lower().str.replace("-", " ").str.replace("  ", " ")
    
    #Borrar localidades duplicadas
    df_pc = df_pc.drop_duplicates(['localidad'], keep='last')
    
    union = pd.merge(right=df_pc, left=df_csv, how='inner', right_on='codigo_postal', left_on='postal_code')
    union = union.drop(columns=['codigo_postal'])
    union = union.rename(columns={'localidad':'location'})
    #print(union.columns.values)
    
    #Cambiando el orden de las columnas:
    union = union.reindex(columns=['ID', 'university', 'career', 'inscription_date', 'first_name', 'last_name','gender', 'age', 'postal_code', 'location', 'email'])
    
    #procesando las últimas columnas:
    union['location'] = union['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")
    union['email'] = union['email'].str.lower().str.replace("-", " ").str.replace("  ", " ")
    union = union.sort_values(by=['ID'])
    print(union.head())
    #Convirtiendo el archivo procesado en .txt
    union.to_csv('/usr/local/airflow/datasets/GHUNBuenosAires_process.txt', sep='\t', index=False)
    
#  Ejecución de DAGS


with DAG(
    'Extract_data_UBuenosAires',
    description='Extraer una base de datos de AWS, procesarla con pandas y retornar mediante S3',
    schedule=timedelta(hours=1),
    start_date=datetime(2022, 11, 2),
    default_args=default_args,
    
) as dag:
    tarea_1 = PythonOperator(task_id='extract_data', 
                             python_callable=extract_data)
    tarea_2 = PythonOperator(task_id='procesamiento_pandas', 
                             python_callable=procesamiento_pandas)
    """tarea_3 = LocalFilesystemToS3Operator(
        task_id='create_local_to_s3_job',
        filename='/usr/local/airflow/files/GHUNBuenosAires_process.txt',
        dest_key='GHUNBuenosAires_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id='aws_s3_bucket',
        replace=True,
)"""
    
tarea_1 >> tarea_2 #>> tarea_3