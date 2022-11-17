from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'juan cometorta'
}

# Definimos el DAG
with DAG(
    dag_id = 'my_example_dag',
    start_date = datetime(2022, 11, 5),
    schedule_interval = '@daily',
    default_args = default_args,
    catchup = True
        ) as dag:
    # Definir los malditos tasks:
    task_1 = BashOperator(
        task_id = 'task_1',
        bash_command = "echo 'omg task 1!'"
    )

    task_2 = BashOperator(
        task_id = 'task_2',
        bash_command = "echo 'omg task 2!'"
    )

    task_3 = BashOperator(
        task_id = 'task_3',
        bash_command = "echo 'omg task 3!'"
    )