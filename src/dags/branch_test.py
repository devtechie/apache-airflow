import sys

sys.path.append("..")
sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/")
sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/Movie/")
sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/")
sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/core/")


from core.execute import core_aggregation, core_db_insert_to_db, core_get_data
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from utils.db import create_connection_object
from utils.variables import random_num
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


SCHEDULE_INTERVAL = '@monthly'

DB_CONN = create_connection_object('postgres_default')

default_args = {
    'owner': 'Business Intelligence',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 16),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

DAG_VERSION = 'branch_test1.0'

dag = DAG(DAG_VERSION
          ,  default_args=default_args
          ,  schedule_interval=SCHEDULE_INTERVAL
          ,  concurrency=1
          ,  max_active_runs=1)


def choose_task(x):
    if x <= 10:
        task = 'aggregation'
    else:
        task = 'db_insert_to_db'
    return task


get_data = PythonOperator(
    task_id='get_data',
    python_callable=core_get_data,
    retries=0,
    provide_context=True,
    dag=dag
)


aggregation = PythonOperator(
    task_id='aggregation',
    python_callable=core_aggregation,
    retries=0,
    provide_context=True,
    dag=dag
)


db_insert_to_db = PythonOperator(
    task_id='db_insert_to_db',
    python_callable=core_db_insert_to_db,
    op_args=[DB_CONN],
    retries=0,
    provide_context=True,
    dag=dag
)

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_task,
    op_args=[int(random_num)],
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag,
)

get_data >> branching

branching >> aggregation >> end
branching >> db_insert_to_db >> end