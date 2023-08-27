from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ExternalPythonOperator',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 12),
    catchup=False,
)


def my_function():
    import sys
    print(f"Running task using {sys.version}")


external_python_task = ExternalPythonOperator(
    task_id='external_python_task',
    python_callable=my_function,
    python='/usr/bin/python3',
    dag=dag,
)


def print_name(name):
    print(f'Hello, {name}')


task_with_args = ExternalPythonOperator(
    task_id='print_name_task',
    python_callable=print_name,
    op_args=['Airflow'],
    python='/usr/bin/python3',
    dag=dag,
)

external_python_task >> task_with_args
