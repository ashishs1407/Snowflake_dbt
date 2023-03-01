
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from utils import (get_env_variables ,
                    connect_to_snowflake ,
                    create_table,insert_data,
                    close_conn,test1)


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 2, 19),
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

with DAG('parameterized',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          template_searchpath=['/opt/airflow/sql_script/jaffle_sql'],
          catchup=False
    ) as dag:

    start = EmptyOperator(task_id='Start')

    tp_test1 = PythonOperator(task_id='tp_test', 
                                python_callable=test1
    )
    # connecting to snowflake
    connect_sf = PythonOperator(task_id='connect_to_snowflake', 
                                python_callable=connect_to_snowflake
    )

    # creating tables 
    
    create_table_in_sf = PythonOperator(task_id='create_table', 
                                python_callable=create_table
    )

    # inserting data into tables from aws s3 bucket
    insert_data_in_sf = PythonOperator(task_id='insert_data', 
                                python_callable=insert_data)
    
    # close connection
    close_connection_sf = PythonOperator(task_id='close_conn', 
                                python_callable=close_conn)
         

    end = EmptyOperator(task_id='END')

    start >> tp_test1 >> connect_sf >> create_table_in_sf >> insert_data_in_sf >> close_connection_sf >>end

