from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from data_transform import read_json_file



default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 2, 13),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}



with DAG('transfer_json_to_snowflake',
          default_args=default_args,
          schedule_interval=timedelta(days=1),
          template_searchpath=['/opt/airflow/sql_script'],
          catchup=False
    ) as dag:

    #read_json_data = PythonOperator(task_id='read_json_data', python_callable=read_json_file)
    start = EmptyOperator(task_id='Start')

    Create_table_in_sf = SnowflakeOperator(task_id='create_table',
    sql = 'create_table_json.sql',
    snowflake_conn_id='snowflake_conn')

    # create_int_stage = SnowflakeOperator(task_id='create_int_stage',
    # sql = 'internal_stage.sql',
    # snowflake_conn_id='snowflake_conn')

    transfer_json_int_stage = SnowflakeOperator(task_id='transfer_json_int_stage',
    sql = 'transfer_json.sql',
    snowflake_conn_id='snowflake_conn')

    copy_into_table = SnowflakeOperator(task_id='copy_into_table',
    sql = 'copy_into_table.sql',
    snowflake_conn_id='snowflake_conn')

    end = EmptyOperator(task_id='End')

start >> Create_table_in_sf >> transfer_json_int_stage >> copy_into_table >> end