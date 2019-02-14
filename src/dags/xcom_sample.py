from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


SCHEDULE_INTERVAL = '@monthly'



default_args = {
    'owner': 'Business Intelligence',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 16),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

DAG_VERSION = 'xcom_test1.0'

dag = DAG(DAG_VERSION
          ,  default_args=default_args
          ,  schedule_interval=SCHEDULE_INTERVAL
          ,  concurrency=1
          ,  max_active_runs=1)


def data_passed(**kwargs):
    task_instance = kwargs['ti']
    list_passed = [1, 2, 3]
    task_instance.xcom_push(key='list_passed', value=list_passed)


def data_received(**kwargs):
    task_instance = kwargs['ti']
    l1 = task_instance.xcom_pull(key='list_passed', task_ids='data_passed')
    return l1


data_passed=PythonOperator(task_id='data_passed',
    python_callable=data_passed,
    retries=0,
    provide_context=True,
    dag=dag)

data_received=PythonOperator(task_id='data_received',
    python_callable=data_received,
    retries=0,
    provide_context=True,
    dag=dag)

data_passed.set_downstream(data_received)