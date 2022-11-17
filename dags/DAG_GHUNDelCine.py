

from airflow import DAG

#  Importar librerias neceasarias
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import os
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


##Funciones

#1.Extraccion de dataset
def extract_data(): 
    #logger.info("... INICIANDO EXTRACCION DE DATASET Y CREACION DE ARCHIVO .CSV ...")
    if "Del_Cine" == "Buenos_Aires": 
        with open(f'/usr/local/airflow/include/GHUNBuenosAires.sql', 'r') as query: 
            sqll = query.read()
        hook = PostgresHook (postgres_conn_id="alkemy_db")
        #logging.info("Exporting query to file")
        df = hook.get_pandas_df(sql=sqll)
        print(df.head())
        try:
            df.to_csv('/usr/local/airflow/files/GHUNBuenosAires_select.csv')
        except:
            print("Archivo existente")
    elif "Del_Cine" == "Del_Cine": 
        with open(f'/usr/local/airflow/include/GHUNDelcine.sql', 'r') as query: 
            sqll = query.read()
        hook = PostgresHook (postgres_conn_id="alkemy_db")
        #logging.info("Exporting query to file")
        df = hook.get_pandas_df(sql=sqll)
        print(df.head())
        try:
            df.to_csv('/usr/local/airflow/files/GHUNDelcine_select.csv')
        except:
            print("Archivo existente")
    #logging.info("... EXTRACCION EXITOSA ...")

#Procesamiento y normalizaciOn de dataset
def procesamiento_pandas():
    
    
    #logging.info("... PROCESANDO Y NORMALIZANDO LA INFORMACION")
    if "Del_Cine" == "Buenos_Aires": 
        #Leer csv y postal_codes
        df_csv = pd.read_csv('/usr/local/airflow/files/GHUNBuenosAires_select.csv')
        df_pc = pd.read_csv('/usr/local/airflow/assets/codigos_postales.csv')
        
        #cambiar nombre de la columna 1 a ID: 
        df_csv = df_csv.rename(columns = {'Unnamed: 0': 'ID'})
        
        #Procesar Columnas 
        df_csv['university'] = df_csv['university'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        
        df_csv['career'] = df_csv['career'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        
        df_csv['inscription_date'] = pd.to_datetime(df_csv['inscription_date'], format='%Y-%m-%d')
        
        
        today = datetime.now()
        year= today.year
        #print("la fecha actual es:", today)
        #print("El anio actual es:", year)
        
        #Calculando la edad a partir de las fechas de nacimiento
        df_csv['fechas_nacimiento'] = pd.to_datetime(df_csv['fechas_nacimiento'], format='%y-%b-%d')
        do = pd.tseries.offsets.DateOffset(years= 100)
        
        #Cambiando los "anios de nacimiento" mayores a 2022 restandoles 100 anios
        df_csv['fechas_nacimiento'] = ((df_csv['fechas_nacimiento']) - do).where(((df_csv['fechas_nacimiento']).dt.year > 2022), df_csv['fechas_nacimiento'])
        print(df_csv['fechas_nacimiento'])
        
        #Calculando los anios: 
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
        
        #Generos
        df_csv['gender'].iloc[df_csv['gender'] == 'm'] = 'male'
        df_csv['gender'].iloc[df_csv['gender'] == 'f'] = 'female'
        
        #Eliminando columnas innecesarias
        col_drop=['fechas_nacimiento', 'location']
        df_csv.drop(columns=col_drop, inplace=True)
        
        #archivo de codigos postales
        df_pc['localidad'] = df_pc['localidad'].astype(str)
        df_pc['localidad'] = df_pc['localidad'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        
        #Borrar localidades duplicadas
        df_pc = df_pc.drop_duplicates(['localidad'], keep='last')
        
        #uniendo los codigos postales con las respectivas localidades
        union = pd.merge(right=df_pc, left=df_csv, how='inner', right_on='codigo_postal', left_on='postal_code')
        
        #Eliminando columnas innecesarias
        union = union.drop(columns=['codigo_postal'])
        union = union.rename(columns={'localidad':'location'})
        #print(union.columns.values)
        
        #Cambiando el orden de las columnas al orden indicado: 
        union = union.reindex(columns=['ID', 'university', 'career', 'inscription_date', 'first_name', 'last_name','gender', 'age', 'postal_code', 'location', 'email'])
        
        #procesando las ultimas columnas: 
        union['location'] = union['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        union['email'] = union['email'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        union = union.sort_values(by=['ID'])
        print(union.head())
        
        #Convirtiendo el archivo procesado en .txt
        union.to_csv('/usr/local/airflow/files/GHUNBuenosAires_process.txt', sep='\t', index=False)

    elif "Del_Cine" == "Del_Cine": 
        #logging.info("... PROCESANDO Y NORMALIZANDO LA INFORMACION")
    
        #Leer csv y postal_codes
        df_csv = pd.read_csv('/usr/local/airflow/files/GHUNDelcine_select.csv')
        df_pc = pd.read_csv('/usr/local/airflow/assets/codigos_postales.csv')
        
        #logging.info("... PROCESANDO Y NORMALIZANDO LA INFORMACION")
        
        #cambiar nombre de la columna 1 a ID:
        df_csv = df_csv.rename(columns = {'Unnamed: 0': 'ID'})
        
        #Procesar Columnas 
        df_csv['university'] = df_csv['university'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        
        df_csv['career'] = df_csv['career'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        
        df_csv['inscription_date'] = pd.to_datetime(df_csv['inscription_date'], format='%Y-%m-%d')
        
        
        today = datetime.now()
        year= today.year
        #print("la fecha actual es:", today)
        #print("El anio actual es:", year)
        print(df_csv['birth_dates'])
        #Calculando la edad a partir de las fechas de nacimiento
        df_csv['birth_dates'] = pd.to_datetime(df_csv['birth_dates'], format='%d-%m-%Y')
        
        #Calculando los anios:
        df_csv['age'] = (today.year - (df_csv['birth_dates']).dt.year) -((today.month) <(df_csv['birth_dates'].dt.month)) -(((today.month) == (df_csv['birth_dates'].dt.month)) & (today.day<df_csv['birth_dates'].dt.day))
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
        
        #Generos
        df_csv['gender'] = df_csv['gender'].iloc[df_csv['gender'] == 'M'] = 'male'
        df_csv['gender'] = df_csv['gender'].iloc[df_csv['gender'] == 'F'] = 'female'
        df_csv['gender']
        
        #Eliminando columnas innecesarias
        col_drop=['birth_dates']
        df_csv.drop(columns=col_drop, inplace=True)
        
        #archivo de codigos postales
        df_pc['localidad'] = df_pc['localidad'].astype(str)
        df_pc['localidad'] = df_pc['localidad'].str.replace("-", " ").str.replace("  ", " ")
        
        #Borrar localidades duplicadas
        df_pc = df_pc.drop_duplicates(['localidad'], keep='last')

        
        #uniendo los codigos postales con las respectivas localidades
        print(df_csv['location'])
        union = pd.merge(right=df_pc, left=df_csv, how='inner', right_on='localidad', left_on='location')
        print(union.head())
        #Eliminando columnas innecesarias
        union = union.drop(columns=['postal_code', 'location'])
        union = union.rename(columns={'localidad':'location', 'codigo_postal':'postal_code'})
        print('los indices son:', union.columns.values)
        
        #Cambiando el orden de las columnas al orden indicado:
        union = union.reindex(columns=['ID', 'university', 'career', 'inscription_date', 'first_name', 'last_name','gender', 'age', 'postal_code', 'location', 'email'])
        
        #procesando las ultimas columnas:
        union['location'] = union['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        union['email'] = union['email'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        union = union.sort_values(by=['ID'])
    
    
         #Convirtiendo el archivo procesado en .txt

        
        #Convirtiendo el archivo procesado en .txt
        union.to_csv('/usr/local/airflow/files/GHUNDelcine_process.txt', sep='\t', index=False)
        #logging.info("... TRANSFORMACION EXITOSA ..." )
        #logging.info("... CARGANDO LOS ARCHIVOS A S3 ...")


#  DAG Args Default
default_args = {  
                'owner': 'andres_s',
                  'retries': 5,  
                  'retry_delay': timedelta(minutes=5)}

#  Ejecucion de DAGS
with DAG(dag_id = "DAG_GHUNDelCine",
         start_date = datetime(2022, 11, 1),
         schedule_interval = "@hourly",
         default_args = default_args,
         catchup = False,
         ) as dag: 
    
    #create_local_to_s3_job(procesamiento_pandas(extract_data(Del_Cine)))

    tarea_1 = PythonOperator(task_id='extract_data', 
                             python_callable=extract_data)
    tarea_2 = PythonOperator(task_id='procesamiento_pandas', 
                             python_callable=procesamiento_pandas)
    
tarea_1 >> tarea_2