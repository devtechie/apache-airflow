import random
import sys
sys.path.append("/usr/local/airflow/core/")
sys.path.append("/usr/local/airflow/config/")
sys.path.append("/usr/local/airflow/")

import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='example_branch_operator',
    default_args=args,
    schedule_interval="@daily",
)

run_this_first = DummyOperator(
    task_id='run_this_first',
    dag=dag,
)

options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=lambda: random.choice(options),
    dag=dag,
)
run_this_first >> branching

join = DummyOperator(
    task_id='join',
    trigger_rule='one_success',
    dag=dag,
)

for option in options:
    t = DummyOperator(
        task_id=option,
        dag=dag,
    )

    dummy_follow = DummyOperator(
        task_id='follow_' + option,
        dag=dag,
    )

    branching >> t >> dummy_follow >> join

# import sys
#
# sys.path.append("..")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/Movie/")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/core/")
#
#
# from core.execute import core_aggregation, core_db_insert_to_db, core_get_data
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from utils.db import create_connection_object
# from airflow.operators.python_operator import BranchPythonOperator
#
# SCHEDULE_INTERVAL = '@hourly'
#
# DB_CONN = create_connection_object('postgres_default')
#
# default_args = {
#     'owner': 'Business Intelligence',
#     'depends_on_past': False,
#     'start_date': datetime(2019, 1, 16),
#     'email_on_failure': True,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5)
# }
#
#
# def decide_which_path():
#     if 1==1 is True:
#         return "aggregation"
#     else:
#         return "db_insert_to_db"
#
#
# DAG_VERSION = 'branch.0'
#
# dag = DAG(DAG_VERSION
#           ,  default_args=default_args
#           ,  schedule_interval=SCHEDULE_INTERVAL
#           ,  concurrency=1
#           ,  max_active_runs=1)
#
#
#
# get_data = PythonOperator(
#     task_id='get_data',
#     python_callable=core_get_data,
#     retries=0,
#     provide_context=True,
#     dag=dag
# )
#
#
# aggregation = PythonOperator(
#     task_id='aggregation',
#     python_callable=core_aggregation,
#     retries=0,
#     provide_context=True,
#     dag=dag
# )
#
#
# db_insert_to_db = PythonOperator(
#     task_id='db_insert_to_db',
#     python_callable=core_db_insert_to_db,
#     op_args=[DB_CONN],
#     retries=0,
#     provide_context=True,
#     dag=dag
# )
#
#
# branching = BranchPythonOperator(
#     task_id='branching',
#     python_callable=decide_which_path,
#     dag=dag,
# )
#
# get_data >> branching
#
# branching >> aggregation
# branching >> db_insert_to_db