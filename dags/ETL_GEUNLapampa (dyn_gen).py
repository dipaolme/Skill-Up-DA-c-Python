import logging
from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
from dateutil.relativedelta import relativedelta
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator
)
import os

# configuración de LOGs
WORKING_DIR = os.getcwd()
LOGS_DIR = os.path.join(WORKING_DIR, 'tests')  
logger = logging.getLogger('GEUNInteramericana') # defino nombre del logger
#Nivel de severidad INFO: el logger solo manejará mensajes de 
# INFO, WARNING, ERROR, y CRITICAL e ignorará mensajes DEBUG.
logger.setLevel(logging.INFO) 
formatter = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')
handler = logging.FileHandler(os.path.join(LOGS_DIR, 'GEUNInteramericana.log'))
handler.setFormatter(formatter)
logger.addHandler(handler)

#defino un diccionario con las variables del DAG
default_args = {
    #'start_date' : datetime(2022, 11, 4),
    #'catchup' : False,
    'retries': 5,
    'owner' : 'alfredo'
    }

# defino la funcion de extraccion y generacion de los csv
def extract_csv():
    #generacion de logs
    #logger.info("Iniciando DAG ...."),
    #logger.info("Iniciando extracción ...."),

    # separo las universidades 
    if "LaPampa" == "Interamericana":
        with open (f'/usr/local/airflow/include/GE_UniInteramericana.sql', 'r') as sqfile:
            query = sqfile.read() 
        hook = PostgresHook(postgres_conn_id="alkemy_db")
        #logging.info("Exporting query to file")
        df = hook.get_pandas_df(sql=query)
        df.to_csv('/usr/local/airflow/tests/GE_Interamericana_select.csv')
        #logger.info("Terminando extracción ....")
    elif "LaPampa" == "LaPampa":
        with open (f'/usr/local/airflow/include/GE_UniLaPampa.sql', 'r') as sqfile:
            query = sqfile.read()
        hook = PostgresHook(postgres_conn_id="alkemy_db")
        #logging.info("Exporting query to file")
        df = hook.get_pandas_df(sql=query)
        df.to_csv('/usr/local/airflow/tests/GE_LaPampa_select.csv'),
        #logger.info("Terminando extracción ....")
    else:
      print("Universidad incorrrecta")


# defino la funcion de transformación
def transform_pandas():
    #generacion de logs
    #logger.info("Iniciando transformacion ....")
    df_cp = pd.read_csv('/usr/local/airflow/tests/codigos_postales.csv')
    #separon las universidades
    if "LaPampa" == "Interamericana":
        #se cargan los datasets
        df = pd.read_csv('/usr/local/airflow/tests/GE_Interamericana_select.csv')
        df = df.rename(columns = {'Unnamed: 0': 'Id'})
        
        df['university'] = df['university'].astype(str)
        df['university'] = df['university'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")
        df['career'] = df['career'].astype(str)
        df['career'] = df['career'].str.lower().str.replace("^-", "").str.replace("-$", "").str.replace(" $", "").str.replace("-", " ").str.replace("  ", " ")
        
        df['last_name'] = df['last_name'].str.lower().str.replace("^mrs.", "").str.replace("^mr.", "").str.replace("^ms.", "").str.replace("^dr.", "").str.replace("^miss", "").str.replace("^ ", "").str.replace("^-", "")
        df['last_name'] = df['last_name'].str.replace("phd$", "").str.replace("md$", "").str.replace("dvm$", "").str.replace("dds$", "").str.replace(" $", "").str.replace("-$", "")
        df['last_name'] = df['last_name'].str.replace("-", " ").str.replace("  ", " ")
        
        new = df['last_name'].str.split(" ", n = 1, expand = True)
        df['first_name']= new[0]
        df['last_name']= new[1]
        df['gender'].iloc[df['gender'] == 'M'] = 'male'
        df['gender'].iloc[df['gender'] == 'F'] = 'female'
        
        df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], format='%y/%b/%d')
        df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%Y-%m-%d')
        
        k = len(df.axes[0])
        for i in range(k):
            if df['fecha_nacimiento'][i].strftime('%Y') >= '2022':
                df['fecha_nacimiento'][i] = df['fecha_nacimiento'][i] - relativedelta(years=100)
        
        df = df.drop(df[df['inscription_date'] <= df['fecha_nacimiento']].index)
        df = df.reset_index(drop=True)
        
        df['inscription_date'] = df['inscription_date'].astype(str)
        df['age'] = datetime.now() - df['fecha_nacimiento']
        df['age'] = (df['age'].dt.days)/365
        df['age'] = df['age'].astype(int)
        df = df.drop(df[df['age'] < 18].index)
        df = df.reset_index(drop=True)
        df = df.drop(['fecha_nacimiento'], axis=1)
        
        df['location'] = df['location'].astype(str)
        df['location'] = df['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        df_cp['localidad'] = df_cp['localidad'].astype(str)
        df_cp['localidad'] = df_cp['localidad'].str.lower().str.replace("-", " ").str.replace("  ", " ")
        df_cp = df_cp.drop_duplicates(['localidad'], keep='last')
    
        inner_df = pd.merge(left=df,right=df_cp, left_on='location', right_on='localidad')
        inner_df = inner_df.drop(['postal_code', 'localidad'], axis=1)
        inner_df = inner_df.rename(columns = {'codigo_postal': 'postal_code'})
        df = inner_df
        df['postal_code'] = df['postal_code'].astype(str)
        df['email'] = df['email'].astype(str)
        df['email'] = df['email'].str.lower().str.replace("-", " ").str.replace(" ", "")
        df.to_csv('/usr/local/airflow/tests/GE_Interamericana_process.txt', sep='\t', index=False)

    elif "LaPampa" == "LaPampa":
        df = pd.read_csv('/usr/local/airflow/tests/GE_LaPampa_select.csv')
        df = df.rename(columns = {'Unnamed: 0': 'Id'})
        
        df['university'] = df['university'].astype(str)
        df['university'] = df['university'].str.lower().str.replace("^-", "").str.replace("-", " ").str.replace("  ", " ")
        df['career'] = df['career'].astype(str)
        df['career'] = df['career'].str.lower().str.replace("^-", "").str.replace("-$", "").str.replace(" $", "").str.replace("-", " ").str.replace("  ", " ")
        
        df['last_name'] = df['last_name'].str.lower().str.replace("^mrs.", "").str.replace("^mr.", "").str.replace("^ms.", "").str.replace("^dr.", "").str.replace("^miss", "").str.replace("^ ", "").str.replace("^-", "")
        df['last_name'] = df['last_name'].str.replace("phd$", "").str.replace("md$", "").str.replace("dvm$", "").str.replace("dds$", "").str.replace(" $", "").str.replace("-$", "")
        df['last_name'] = df['last_name'].str.replace("-", " ").str.replace("  ", " ")
        new = df['last_name'].str.split(" ", n = 1, expand = True)
        df['first_name']= new[0]
        df['last_name']= new[1]
    
        df['gender'].iloc[df['gender'] == 'M'] = 'male'
        df['gender'].iloc[df['gender'] == 'F'] = 'female'
        
        df['fecha_nacimiento'] = pd.to_datetime(df['fecha_nacimiento'], format='%d/%m/%Y')
        df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%Y-%m-%d')
        df = df.drop(df[df['inscription_date'] <= df['fecha_nacimiento']].index)
        df = df.reset_index(drop=True)
        df['inscription_date'] = df['inscription_date'].astype(str)
        
        df['age'] = datetime.now() - df['fecha_nacimiento']
        df['age'] = (df['age'].dt.days)/365
        df['age'] = df['age'].astype(int)
        df = df.drop(df[df['age'] < 18].index)
        df = df.reset_index(drop=True)
        df = df.drop(['fecha_nacimiento'], axis=1)
        
        inner_df = pd.merge(left=df,right=df_cp, left_on='postal_code', right_on='codigo_postal')
        inner_df = inner_df.drop(['location', 'codigo_postal'], axis=1)
        inner_df = inner_df.rename(columns = {'localidad': 'location'})
        df = inner_df
        df['location'] = df['location'].astype(str)
        df['location'] = df['location'].str.lower().str.replace("-", " ").str.replace("  ", " ")
    
        df['email'] = df['email'].astype(str)
        df['email'] = df['email'].str.lower().str.replace("-", " ").str.replace(" ", "")
        df['postal_code'] = df['postal_code'].astype(str)
        df.to_csv('/usr/local/airflow/tests/GE_LaPampa_process.txt', sep='\t', index=False)

    else:
        print("Universidad incorrrecta")

    #logger.info("Terminando transformacion ....")
    #logger.info("Iniciando carga ....")

# se define el DAG
with DAG(
    dag_id = "ETL_GEUNLapampa_dyn", 
    start_date=datetime(2022, 11, 4), 
    schedule_interval="@hourly", 
    default_args=default_args, 
    catchup=False,
    tags = ['ETL creado con DAG dinmaico']

) as dag:

# se definen los tasks
    tarea_1 = PythonOperator(
        task_id = "extract",
        python_callable = extract_csv   # para ejecutar una función se llama con python_callable
        #op_args = [path_sql, path_csv]
    
    )

    tarea_2 = PythonOperator(
        task_id = "transform",
        python_callable = transform_pandas
    )

    tarea_3 = LocalFilesystemToS3Operator(
        task_id = "load",
        #filename='/usr/local/airflow/tests/GE_Interamericana_process.txt',
        filename= os.path.join(os.getcwd, 'GE_'+ 'LaPampa'+'.txt'),
        dest_key='GEUN'+'LaPampa'+'_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id="aws_s3_bucket",
        replace=True

    )

# se definen las dependencias de las tareas
    tarea_1 >> tarea_2 >> tarea_3