from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator


def get_variable():
    my_var = Variable.get('my_env_var')
    print('Variable is ' + my_var)
    return my_var


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3, 6),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


with DAG('env_var_prac',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          catchup=False
    ) as dag:


    var_get = PythonOperator(task_id='var_get',
                              python_callable=get_variable)

   