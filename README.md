# Snowflake_dbt

# to copy a file from container to local system
docker cp 17b4135ea9ab:/opt/airflow/airflow.cfg "D:\Colgate_training\Snowflake_dbt"
docker cp <container_id>:<complete path of file in container> "path to local system"


# enter the postgres container 
docker exec -it <container_id> psql -d airflow -U airflow

# once inside psql
type 
\dt # need to hit enter to list complete table
\d connection

# OR execute this line
 docker exec -it snowflake_dbt-postgres-1 psql -d airflow -U airflow -c "SELECT * FROM connection WHERE conn_id='snowflake_conn'"

 # python script to generate fernet key run this script in webserver container
 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

 # it will generate a key like this
 gZV0G6fnfe2zl4k785PIDNNcrQb1REsx2Oc_umnlsys=

 # restart a container
 docker compose  -f "docker-compose.yaml" up -d --build airflow-webserver
 or
 docker compose -f docker-compose.yaml restart airflow-webserver

 # Backfill 
 enter the container \n
 airflow backfill -s <start date yyyy-mm-dd> -e <yyyy-mm-dd> --rerun_failed_tasks -B backfill