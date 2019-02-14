import sys
#
# sys.path.append("..")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/Movie/")
sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/")
# sys.path.append("/Users/nvishwakarma/Downloads/airflow-datapipeline-demo1/src/core/")
#
# from core.execute import core_aggregation, core_db_insert_to_db, core_get_data
from core.subdag import subdag as s
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from utils.db import create_connection_object
from airflow.operators.dummy_operator import DummyOperator
# import airflow
# from airflow.example_dags.subdags.subdag import subdag
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator


DAG_NAME = 'subdag_test_original.0'
# DB_CONN = create_connection_object('postgres_default')

filename_num = {1: r"//Files//file1.csv"
    , 2: r"//Files//file2.csv"}

args = {
    'owner': 'airflow',
    'start_date': (2019, 1, 23),
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval="@once",
)



#
# # get_data = PythonOperator(
# #     task_id='get_data',
# #     python_callable=core_get_data,
# #     retries=0,
# #     provide_context=True,
# #     dag=dag
# # )

start = DummyOperator(
    task_id='start',
    default_args=args,
    dag=dag,
)


process_file = SubDagOperator(
    task_id='process_file',
    subdag=s(DAG_NAME, 'process_file', args),
    op_args=[filename_num],
    default_args=args,
    dag=dag,
)


# # db_insert_to_db = PythonOperator(
# #     task_id='db_insert_to_db',
# #     python_callable=core_db_insert_to_db,
# #     op_args=[DB_CONN],
# #     retries=0,
# #     provide_context=True,
# #     dag=dag
# # )
#
end = DummyOperator(
    task_id='end',
    default_args=args,
    dag=dag,
)

start >> process_file >> end