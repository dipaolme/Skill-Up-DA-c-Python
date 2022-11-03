#import logging
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


POSTGRES_CONN_ID = "alkemy_db"

def pg_extract2csv(copy_sql):
  pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
  #logging.info("Exporting query to file")
  #pg_hook.copy_expert(sql=f'/usr/local/airflow/include/{query}', filename="/usr/local/airflow/files/GAUflores_select.csv")
  pg_hook.copy_expert(copy_sql, filename="/usr/local/airflow/files/GAUflores_select.csv")


# Instantiate DAG
with DAG(
    "GAUflores_ETL",
    start_date=datetime(2022, 3, 11),
    max_active_runs=3,
    schedule_interval="@daily",
    #default_args={
    #    "email_on_failure": False,
    #    "email_on_retry": False,
    #    "retries": 1,
    #    "retry_delay": timedelta(minutes=1),
    #},
    catchup=False,
    #template_searchpath="/include/"
) as dag:
    extract = PythonOperator(
        task_id="extract_task",
        python_callable=pg_extract2csv
        op_kwargs={"copy_sql": "COPY(select universidad, carrera, fecha_de_inscripcion, name, nombre_de_usuario, sexo, codigo_postal, correo_electronico  from flores_comahue where universidad ='UNIVERSIDAD DE FLORES' and fecha_de_inscripcion between '2020-09-01' and '2021-02-01')TO STDOUT WITH CSV HEADER"
        }
    )
 
    extract