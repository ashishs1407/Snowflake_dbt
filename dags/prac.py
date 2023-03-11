from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

def say_hi(ti):
    return ti.xcom_push(key="hi",value="hi...!!!")

def say_hello(ti):
   return ti.xcom_pull(key="hi",task_ids='task1')

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3,9),
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}



with DAG('prac',
          default_args=default_args,
          schedule_interval=timedelta(days=1)
    ) as dag:

    start = EmptyOperator(task_id='Start')

    task1 = PythonOperator(task_id = "task1",python_callable=say_hi)

    task2 = PythonOperator(task_id = "task2",python_callable=say_hello)
     
    end =  EmptyOperator(task_id='End')

    start >> task1 >> task2 >> end