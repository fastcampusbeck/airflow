from datetime import timedelta, datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator, PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ShortCircuitOperator',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 12),
    catchup=False,
)


def condition_check():
    return True


def run_this_func():
    print('run_this_func executed!')


condition_task = ShortCircuitOperator(
    task_id='condition_check',
    python_callable=condition_check,
    dag=dag
)

run_this_task = PythonOperator(
    task_id='run_this',
    python_callable=run_this_func,
    provide_context=True,
    dag=dag
)


def receive_user_input(**kwargs):
    user_input_value = Variable.get('user_input_value', default_var=0)

    if user_input_value:
        return int(user_input_value)
    else:
        print("No value set for 'user input value' variable.")
        return 0


receive_input_task = PythonOperator(
    task_id='receive_user_input',
    python_callable=receive_user_input,
    provide_context=True
)


def complex_condition_check(**kwargs):
    ti = kwargs['ti']
    received_value = ti.xcom_pull(task_ids='receive_user_input', dag_id='ShortCircuitOperator')
    if received_value > 10:
        return True
    return False


complex_condition_task = ShortCircuitOperator(
    task_id='complex_condition_check',
    python_callable=complex_condition_check,
    provide_context=True,
    dag=dag,
)


receive_input_task >> complex_condition_task >> run_this_task

