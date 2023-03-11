from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}

def _training_model(ti):
    accuracy = uniform(0.1, 1.0)
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key="model_acc",value=accuracy)


def _choose_best_model(ti):
    ma = ti.xcom_pull(key="model_acc",task_ids='processing_tasks.training_model_a')
    mb = ti.xcom_pull(key="model_acc",task_ids='processing_tasks.training_model_b')
    mc = ti.xcom_pull(key="model_acc",task_ids='processing_tasks.training_model_c')
    
    dict1 ={"model_a":ma,"model_b":mb,'model_c':mc}
    print(dict1)
    largest=0
    for x,y in dict1.items():
        if y >= largest:
            modelx = x
            largest = y

    print(f'choose best model is : {modelx} with accuracy of {largest}')        

        

    
    

with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3'
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model,
            provide_context=True
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model,
            provide_context=True
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model,
            provide_context=True
        )

    choose_model = PythonOperator(
        task_id='task_4',
        python_callable=_choose_best_model,
        provide_context=True
    )

    downloading_data >> processing_tasks >> choose_model