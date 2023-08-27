import os.path
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'PythonSensor',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 12),
    catchup=False,
)


def check_file_exists():
    return True if os.path.exists('/opt/airflow/dags/python_operator/dummy.txt') else False


wait_for_file = PythonSensor(
    task_id='wait_for_file',
    python_callable=check_file_exists,
    timeout=600,
    poke_interval=30,
    mode='poke',
    dag=dag,
)


def check_api_response():
    response = requests.get('http://localhost/data')
    return True if response.status_code == 200 else False


wait_for_api = PythonSensor(
    task_id='wait_for_api',
    python_callable=check_api_response,
    timeout=300,
    poke_interval=10,
    mode='poke',
    soft_fail=True,
    dag=dag
)


def check_current_second():
    return True if datetime.now().second // 2 == 0 else False


wait_with_poke = PythonSensor(
    task_id='wait_with_poke',
    python_callable=check_current_second,
    timeout=60,
    poke_interval=10,
    mode='poke',
    dag=dag,
)

wait_with_reschedule = PythonSensor(
    task_id='wait_with_reschedule',
    python_callable=check_current_second,
    timeout=60,
    poke_interval=10,
    mode='reschedule',
    dag=dag
)


def run_after_sensor():
    print('run_after_sensor executed!')


dummy_task_poke = PythonOperator(
    task_id='run_after_poke_task',
    python_callable=run_after_sensor,
    dag=dag,
)

dummy_task_reschedule = PythonOperator(
    task_id='run_after_reschedule_task',
    python_callable=run_after_sensor,
    dag=dag,
)

wait_with_poke >> dummy_task_poke
wait_with_reschedule >> dummy_task_reschedule
