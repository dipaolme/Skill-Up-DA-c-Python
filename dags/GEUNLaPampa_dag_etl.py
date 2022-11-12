import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
import pandas as pd
import os

# configuración de LOGs
WORKING_DIR = os.getcwd()
LOGS_DIR = os.path.join(WORKING_DIR, 'tests')  
logger = logging.getLogger('GEUNLaPampa') # defino nombre del logger
#Nivel de severidad INFO: el logger solo manejará mensajes de 
# INFO, WARNING, ERROR, y CRITICAL e ignorará mensajes DEBUG.
logger.setLevel(logging.INFO) 
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')
handler = logging.FileHandler(os.path.join(LOGS_DIR, 'GEUNLaPampa.log'))
handler.setFormatter(formatter)
logger.addHandler(handler)

#defino un diccionario con las variables del DAG
default_args = {
    #'schedule_interval' : "@hourly",  NO VA ACA
    'start_date' : datetime(2022, 11, 4),
    'catchup' : False,
    'retries': 5,
    'owner' : 'alfredo'
    }

# defino la funcion de extraccion y generacion de los csv
def extract_csv():
    #generacion de logs en GEUNLaPampa.log
    #logger.info("Iniciando DAG ...."),
    #logger.info("Iniciando extracción ...."),
    with open (f'/usr/local/airflow/include/GE_UniLaPampa.sql', 'r') as sqfile:
        query = sqfile.read() 
    hook = PostgresHook(postgres_conn_id="alkemy_db")
    #logging.info("Exporting query to file")
    df = hook.get_pandas_df(sql=query)
    df.to_csv('/usr/local/airflow/tests/GE_LaPampa_select.csv'),
    #logger.info("Terminando extracción ....")
    
# defino la funcion de transformación
def transform_pandas():
    #generacion de logs en GEUNLaPampa.log
    #logger.info("Iniciando transformacion ....")
    # se cargan los datasetes
    df_cp = pd.read_csv('/usr/local/airflow/tests/codigos_postales.csv')
    df = pd.read_csv('/usr/local/airflow/tests/GE_LaPampa_select.csv')
    #renombro la columna 'Unnamed: 0' por 'Id'
    df = df.rename(columns = {'Unnamed: 0': 'Id'})

    df['university'] = df['university'].astype(str)
    df['university'] = df['university'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")

    df['career'] = df['career'].astype(str)
    df['career'] = df['career'].str.lower().str.replace("^-", "").str.replace("-$", "").str.replace(" $", "").str.replace("-", " ").str.replace("  ", " ")

    # se extraen los espacios y caracteres que no sirven
    df['last_name'] = df['last_name'].str.lower().str.replace("^mrs.", "").str.replace("^mr.", "").str.replace("^ms.", "").str.replace("^dr.", "").str.replace("^miss", "").str.replace("^ ", "").str.replace("^-", "")
    df['last_name'] = df['last_name'].str.replace("phd$", "").str.replace("md$", "").str.replace("dvm$", "").str.replace("dds$", "").str.replace(" $", "").str.replace("-$", "")
    df['last_name'] = df['last_name'].str.replace("-", " ").str.replace("  ", " ")
    # separo nombre de apellido (todo guardado en last_name)
    new = df['last_name'].str.split(" ", n = 1, expand = True)
    df['first_name']= new[0]
    df['last_name']= new[1]

    df['gender'].iloc[df['gender'] == 'M'] = 'male'
    df['gender'].iloc[df['gender'] == 'F'] = 'female'

    # convierto las columnas en formato fecha
    df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], format='%d/%m/%Y')
    df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%Y-%m-%d')
    # elimino las filas con fecha de nacimiento mayor a la fecha de inscripcion y reseteo indices
    df = df.drop(df[df['inscription_date'] <= df['fecha_nacimiento']].index)
    df = df.reset_index(drop=True)
 
    # vuelvo a convertir a 'inscription_date' en string
    df['inscription_date'] = df['inscription_date'].astype(str)

    # Cálculo de la edad
    df['age'] = datetime.now() - df['fecha_nacimiento']
    df['age'] = (df['age'].dt.days)/365
    df['age'] = df['age'].astype(int)
    # elimino los datos con edades menores a 18 y reseteo índice 
    df = df.drop(df[df['age'] < 18].index)
    df = df.reset_index(drop=True)
    # elimino la columna fecha de nacimiento
    df = df.drop(['fecha_nacimiento'], axis=1)

    # inner join para obtener la localidad a partir del código postal
    inner_df = pd.merge(left=df,right=df_cp, left_on='postal_code', right_on='codigo_postal')
    # elimino las columnas 'location' y 'codigo_postal'
    inner_df = inner_df.drop(['location', 'codigo_postal'], axis=1)
    #renombro columna localidad
    inner_df = inner_df.rename(columns = {'localidad': 'location'})
    df = inner_df
    # paso location a string (ya está, pero por las dudas)
    # lo llevo todo a minusculas, elimino espacios extras y elimino guiones
    df['location'] = df['location'].astype(str)
    df['location'] = df['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")

    # paso email a string (ya está, pero por las dudas)
    # lo llevo todo a minusculas, elimino espacios extras y elimino guiones
    df['email'] = df['email'].astype(str)
    df['email'] = df['email'].str.lower().str.replace("-", " ").str.replace(" ", "")

    # llevo el campo postal_code a string
    df['postal_code'] = df['postal_code'].astype(str)

    #Pasaje de dataframe a archivo de texto
    df.to_csv('/usr/local/airflow/tests/GE_LaPampa_process.txt', sep='\t', index=False)

    #logger.info("Terminando transformacion ....")
    #logger.info("Iniciando carga ....")

# se define el DAG
with DAG(
    dag_id = "DAG_Uni_LaPampa_ETL_log",
    default_args = default_args,
    schedule_interval="@hourly",
    tags = ['ETL Universidad La Pampa (extract&transform&load) con log']
) as dag:

# se definen los tasks
    tarea_1 = PythonOperator(
        #logger.info("Iniciando extraccion"),
        task_id = "extract",
        python_callable = extract_csv   # para ejecutar una función se llama con python_callable
            
    )

    tarea_2 = PythonOperator(
        #logger.info("Iniciando transformacion"),
        task_id = "transform",
        python_callable = transform_pandas
        
    )

    tarea_3 = LocalFilesystemToS3Operator(
        #logger.info("Iniciando carga"),
        task_id = "load",
        filename='/usr/local/airflow/tests/GE_LaPampa_process.txt',
        dest_key='GEUNLaPampa_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id="aws_s3_bucket",
        replace=True
    )
    

# se definen las dependencias de las tareas
    tarea_1 >> tarea_2 >> tarea_3