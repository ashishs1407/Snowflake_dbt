from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email import EmailOperator



default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2023, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}



dag = DAG('send_data_to_snowflake_docker',
          default_args=default_args,
          schedule='@daily',
          template_searchpath=['/opt/airflow/sql_script'],
          #catchup=False
          )



create_table = SnowflakeOperator(
    task_id='create_table',
    sql = 'create_table.sql',
    snowflake_conn_id='snowflake_conn',
    dag=dag)

insert_into_table = SnowflakeOperator(
    task_id='insert_table',
    sql = 'insert_data.sql',
    snowflake_conn_id='snowflake_conn',
    dag=dag )

# send_mail = EmailOperator(task_id='send_email',
#         to='shimpi.ashish.me2@gmail.com',
#         cc= 'mandar.dhumal@diacto.com',
#         subject='Daily report generated',
#         html_content=""" <h1>Congratulations! Your store reports are ready.</h1> """,
#         conn_id='gmail'
#         dag=dag)


create_table >> insert_into_table

