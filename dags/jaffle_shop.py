
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from data_transform import read_json_file

default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 2, 14),
    'retries': 0,
    'retry_delay': timedelta(seconds=5)
}

with DAG('jaffle_shop',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          template_searchpath=['/opt/airflow/sql_script/jaffle_sql'],
          catchup=False
    ) as dag:

    start = EmptyOperator(task_id='Start')

    # creating tables 
    with TaskGroup('create_tables') as create_tables:
        Create_table_customer = SnowflakeOperator(task_id='Create_table_customer',
            sql = 'create_customer_table.sql',
            snowflake_conn_id='snowflake_conn')

        Create_table_orders = SnowflakeOperator(task_id='Create_table_orders',
            sql = 'create_order_table.sql',
            snowflake_conn_id='snowflake_conn')

        Create_table_payments = SnowflakeOperator(task_id='Create_table_payments',
            sql = 'create_payment.sql',
            snowflake_conn_id='snowflake_conn')

    # inserting data into tables from aws s3 bucket
    with TaskGroup('insert_data') as insert_data:
        insert_customer_data = SnowflakeOperator(task_id='insert_customer_data',
            sql = 'insert_customer.sql',
            snowflake_conn_id='snowflake_conn')
        
        insert_order_data = SnowflakeOperator(task_id='insert_order_data',
            sql = 'insert_order.sql',
            snowflake_conn_id='snowflake_conn')
        
        insert_payment_data = SnowflakeOperator(task_id='insert_payment_data',
            sql = 'insert_payment.sql',
            snowflake_conn_id='snowflake_conn')

    end = EmptyOperator(task_id='END')

    start >> create_tables >> insert_data >> end

