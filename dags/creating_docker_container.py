from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 9),
    'retries': 0
}

dag = DAG(
    'build_python_app_docker_image',
    default_args=default_args,
    description='Build a Docker image of Python application',
    schedule_interval='@daily',
)

set_working_directory_task = BashOperator(
    task_id='set_working_directory',
    bash_command='cd /opt/airflow/app',
    dag=dag,
)

check_docker_version = BashOperator(
    task_id='check_docker_version',
    bash_command='docker --version',
    dag=dag,
)
# Define the task that builds the Docker image
build_docker_image_task = BashOperator(
    task_id='build_docker_image',
    bash_command='docker build -t my_python_app:latest .',
    dag=dag,
)


set_working_directory_task >> check_docker_version >> build_docker_image_task
