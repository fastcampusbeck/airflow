from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'PythonVirtualEnvOperator',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 12),
    catchup=False,
)


def virtual_function():
    from time import sleep
    print("Sleeping for 5 seconds...")
    sleep(5)
    print("Woke up!")


virtualenv_task = PythonVirtualenvOperator(
    task_id='basic_virtualenv_task',
    python_callable=virtual_function,
    dag=dag,
)


def color_function():
    from colorama import Fore
    print(Fore.RED + "This is red!")


virtualenv_with_package_task = PythonVirtualenvOperator(
    task_id='virtualenv_with_package_task',
    python_callable=color_function,
    requirements=["colorama==0.4.0"],
    dag=dag
)


def system_package_function():
    import sys
    print(sys.version)


virtualenv_with_system_packages_task = PythonVirtualenvOperator(
    task_id='virtualenv_with_system_packages_task',
    python_callable=system_package_function,
    system_site_packages=True,
    dag=dag
)

virtualenv_task >> virtualenv_with_package_task >> virtualenv_with_system_packages_task
