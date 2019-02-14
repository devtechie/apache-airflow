import sys
sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/")
import airflow
# from airflow.example_dags.subdags.subdag import subdag
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from core.test import hello_method


DAG_NAME = 'hello_example1.0'

args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 16),
}


dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval=None,
)

hello = PythonOperator(
    task_id='hello',
    python_callable=hello_method,
    provide_context=True,
    dag=dag,
)
