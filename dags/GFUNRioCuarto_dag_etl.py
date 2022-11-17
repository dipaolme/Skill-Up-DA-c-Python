from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from datetime import timedelta, datetime, date
from pathlib import Path
import pandas as pd
import logging, os


# Logging config 

LOGS_DIR = 'include'
LOGGER = logging.getLogger('GFUNRioCuarto')
LOGGER.setLevel(logging.INFO)

FORMATTER = logging.Formatter('%(asctime)s - %(name)s - %(message)s', '%Y-%m-%d')

HANDLER = logging.FileHandler(os.path.join(LOGS_DIR, 'GFUNRioCuarto.log'))
HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(HANDLER)


# Exctraccion de los datos con la query
def extract(**kwargs):
    LOGGER.info('Starting extract process...')

    query_path = kwargs['path']
    hook = PostgresHook(postgres_conn_id='alkemy_db')
    
    file_name = kwargs['name']

    with open(query_path, 'r') as f:
        df = hook.get_pandas_df(sql=f.read())
        filepath = Path('files/' + file_name + '_select.csv')
        df.to_csv(filepath)
        f.close()

    LOGGER.info('Success')

# Transforma strings al formato estandar pedido
def legible(df_string):
    return df_string.str.replace('-', ' ').str.replace('_', ' ').str.lower()

# Calcula una edad dada una fecha
def age(birthdate):
    today = date.today()
    age = today.year - birthdate.year - ((today.month, today.day) < (birthdate.month, birthdate.day))
    return age

# Transformacion de los datos
def transform_data():
    LOGGER.info("Starting transform process...")

    # Carga de los datasets
    df = pd.read_csv('files/GFUNRioCuarto_select.csv')
    cod_postales = pd.read_csv('assets/codigos_postales.csv')
    cod_postales['codigo_postal'] = cod_postales['codigo_postal'].astype(str)
    cod_postales['localidad'] = legible(cod_postales['localidad'])

    # age pasa de str a datetime y despues calculo la edad con age()
    df['age']               = pd.to_datetime(df['age'], format='%y/%b/%d')
    df['age']               = df['age'].apply(age)
    df                      = df.drop(df[(df['age'] < 18)].index)

    # Normalizo los datos con legible()
    df['university']        = legible(df['university'])
    df['career']            = legible(df['career'])
    df['email']             = legible(df['email'])
    df['location']          = legible(df['location'])

    # inscription_date pasa de str a datetime y despues a formato %Y-%m-%d (2022-01-01)
    df['inscription_date']  = pd.to_datetime(df['inscription_date'])
    df['inscription_date']  = df['inscription_date'].dt.strftime('%Y-%m-%d')

    # M -> male | F -> female
    df['gender']            = df['gender'].replace(['M','F'],['male','female'])

    # droppeo los duplicados en cod_postales['localidad'] para evitar errores antes de asignar los cp
    cod_postales            = cod_postales.drop_duplicates(subset='localidad', keep="first")
    df['postal_code']       = df['postal_code'].astype(str)
    df['postal_code']       = df.join(cod_postales.set_index('localidad'), on='location')['codigo_postal']

    # legible() para normalizar los nombres
    df.first_name           = legible(df.first_name)
    aux_df                  = df['first_name'].str.split(" ", 1, expand=True)
    df['first_name']        = aux_df[0]
    df['last_name']         = aux_df[1]
    

    df.to_csv('datasets/GFUNRioCuarto_process.txt', index=None)

    LOGGER.info('Success')
    LOGGER.info('Uploading data to s3 bucket...')

    return




with DAG(
    dag_id='GFUNRioCuarto',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 7)
) as dag:

    exctract = PythonOperator(
        task_id='extraer',
        python_callable=extract,
        op_kwargs={'path': 'include/GFUNRioCuarto.sql',
                   'name': 'GFUNRioCuarto'}
    )

    transform = PythonOperator(
        task_id='normalizar',
        python_callable=transform_data,
    )

    s3_load = LocalFilesystemToS3Operator(
        task_id = "load",
        filename='datasets/GFUNRioCuarto_process.txt',
        dest_key='GFUNRioCuarto_process.txt',
        dest_bucket='dipa-s3',
        aws_conn_id="aws_s3_bucket",
        replace=True
    )


    exctract >> transform >> s3_load