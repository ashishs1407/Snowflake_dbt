from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from data_transform import stock_data,stock_data_par



default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 3,4),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}



with DAG('stock_api_data_to_snowflake',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          template_searchpath=['/opt/airflow/sql_script/parquet_sql'],
          catchup=False
    ) as dag:

    start = EmptyOperator(task_id='Start')  # dummyoperator 

    with TaskGroup('pull_data_and_save') as pull_data_and_save :

        pull_api_json_data = PythonOperator(task_id='pull_api_json_data', python_callable=stock_data)

        pull_api_parquet_data = PythonOperator(task_id='pull_api_parquet_data', python_callable=stock_data_par)
    
    Create_table_in_sf = SnowflakeOperator(task_id='create_table',
        sql = 'create_table.sql',
        snowflake_conn_id='snowflake_conn')

    create_int_stage = SnowflakeOperator(task_id='create_int_stage',
        sql = 'create_int_stage.sql',
        snowflake_conn_id='snowflake_conn')

    transfer_json_int_stage = SnowflakeOperator(task_id='transfer_json_int_stage',
        sql = 'put_int_stage.sql',
        snowflake_conn_id='snowflake_conn')
    

    copy_into_table = SnowflakeOperator(task_id='copy_into_table',
        sql = 'copy_into_table.sql',
        snowflake_conn_id='snowflake_conn')

    end = EmptyOperator(task_id='End')

    start >> pull_data_and_save >> Create_table_in_sf >> create_int_stage >> transfer_json_int_stage >> copy_into_table >> end