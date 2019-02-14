from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
import pandas as pd


def repeat(filename: str, **kwargs):
    task_instance = kwargs['ti']
    df = pd.read_csv(filename)
    df1 = df[df['customer_name'] == 'JUPITER']
    df1 = df1.to_csv(path_or_buf="/Users/nvishwakarma/Downloads/final.csv", sep="|", mode='a', header=False)


def subdag(parent_dag_name, child_dag_name, args,filename_num, **kwargs):
    task_instance = kwargs['ti']
    # task_instance.
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args, schedule_interval=None)

    for key, val in filename_num.items():
        PythonOperator(
            task_id='%s-task-%s' % (child_dag_name, key),
            python_callable=repeat,
            op_args=[val],
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag

