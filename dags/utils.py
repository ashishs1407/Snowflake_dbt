import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_env_variables():
    import os,json

    envar = os.getenv('SF_CONN')
    json_obj = json.loads(envar)
    # print(type(json_obj))
    return json_obj


def connect_to_snowflake():

    try:
        # import os,json

        # envar = os.getenv('SF_CONN')
        # json_obj = json.loads(envar)
        # print(type(json_obj))
        

        # Connect to Snowflake
        #json_obj = get_env_variables()

        #extra = json_obj['extra']
        con = snowflake.connector.connect(
            user='ASHISH1407',
            password='Vaishnavi@1407',
            account='yq88348.ap-southeast-1',
            database='MY_DB',
            schema='MY_SCHEMA',
            warehouse='COMPUTE_WH'
        # user=json_obj["login"],
        # password=json_obj['password'],
        # account=extra['account'],
        # database=extra['database'],
        # schema=json_obj['schema'],
        # warehouse=extra['warehouse']
        )

        print("connection established succesfully")
        cursor = con.cursor()

    
        cursor.execute(""" 
        create table if not exists parameterized_test 
            ( id integer,
            first_name varchar,
            last_name varchar
            );
        """)

        return cursor
    except:
        print('connection failed please check connection parameters')



def create_table():
    cursor = connect_to_snowflake()
    cursor.execute(""" 
     create table if not exists parameterized_test 
        ( id integer,
        first_name varchar,
        last_name varchar
        );
       """)



def insert_data():
    cursor = connect_to_snowflake()
    cursor.execute(""""
    INSERT INTO Employee_airflow_dock (name, department, salary, hire_date)
    VALUES 
    ('John Doe3', 'IT', 75000, '2020-01-01'),
    ('Jane Doe3', 'HR', 65000, '2021-03-01'),
    ('Bob Smith3', 'Sales', 55000, '2022-06-01');
    """)


def close_conn():
    cursor = connect_to_snowflake()
    cursor.close()


    
    
def test1():
    return 100