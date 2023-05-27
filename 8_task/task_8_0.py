from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group
import snowflake.connector


SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSSWORD = os.getenv("SNOWFLAKE_PASSSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

filename = "/home/keruiiia/airflow/data/763K_plus_IOS_Apps_Info.csv"

connection = snowflake.connector.connect(
    user = SNOWFLAKE_USER,
    password = SNOWFLAKE_PASSSWORD,
    account = SNOWFLAKE_ACCOUNT,
    warehouse = SNOWFLAKE_WAREHOUSE,
    database = SNOWFLAKE_DATABASE,
    schema = SNOWFLAKE_SCHEMA
)

def parse_csv(**context):
    data = pd.read_csv(filename)
    data = data.reset_index(drop=True)
    data.to_csv('/home/keruiiia/airflow/data/parsed_data.csv', index=False)
	
	
def create_data_streams():
    cursor = connection.cursor()
    
    cursor.execute("CREATE OR REPLACE STREAM RAW_STREAM ON TABLE RAW_TABLE")
    cursor.execute("CREATE OR REPLACE STREAM STAGE_STREAM ON TABLE STAGE_TABLE")
    
    cursor.close()
    connection.close()
	
	
def create_stage(**context):
    cursor = connection.cursor()

    cursor.execute("""create or replace file format my_csv_format
        type = csv
        record_delimiter = '\n'
        field_delimiter = ','
        skip_header = 1
        null_if = ('NULL', 'null')
        empty_field_as_null = true
        FIELD_OPTIONALLY_ENCLOSED_BY = '0x22'""")
	
    cursor.execute("CREATE OR REPLACE STAGE my_stage FILE_FORMAT = my_csv_format")

    cursor.close()
    connection.close()
	
	
def upload_file_to_stage():
    cursor = connection.cursor()
    local_file_path = '/home/keruiiia/airflow/data/parsed_data.csv'
    stage_name = 'my_stage'
	
    cursor.execute(f"PUT 'file://{local_file_path}' @{stage_name}")
	
    cursor.close()
    connection.close()
	
	
def write_to_raw_table():
    cursor = connection.cursor()
    stage_name = 'my_stage'
    
    cursor.execute(f"COPY INTO RAW_TABLE FROM @{stage_name}/parsed_data.csv FILE_FORMAT = (FORMAT_NAME = my_csv_format)")
    
    cursor.close()
    connection.close()


def write_to_stage_table():
    cursor = connection.cursor()
    
    # getting a list of columns from a RAW_TABLE 
    cursor.execute("DESCRIBE TABLE RAW_TABLE")
    cols = cursor.fetchall()
    
    # creating a List of column names
    col_names = ', '.join([col[0] for col in cols])
    
    # SQL query to insert data
    sql = f"INSERT INTO STAGE_TABLE ({col_names}) SELECT {col_names} FROM RAW_STREAM"
    
    cursor.execute(sql)
    
    cursor.close()
    connection.close()

	
def write_to_master_table():
    cursor = connection.cursor()
    
    # getting a list of columns from a STAGE_TABLE 
    cursor.execute("DESCRIBE TABLE STAGE_TABLE")
    cols = cursor.fetchall()
    
    # creating a List of column names
    col_names = ', '.join([col[0] for col in cols])
    
    # SQL query to insert data
    sql = f"INSERT INTO MASTER_TABLE ({col_names}) SELECT {col_names} FROM STAGE_STREAM"
    
    cursor.execute(sql)
    
    cursor.close()
    connection.close()


with DAG('elt_pipeline',
        start_date=datetime(2023, 5, 20),
        schedule_interval=None,
        catchup=False) as dag:

    parse_csv_task = PythonOperator(
        task_id='parse_csv',
        python_callable=parse_csv,
        provide_context=True,
        dag=dag
	)

    create_data_streams_task = PythonOperator(
        task_id='create_data_streams',
        python_callable=create_data_streams,
        provide_context=True,
        dag=dag
    )
	
    create_stage_task = PythonOperator(
        task_id='create_stage',
        python_callable=create_stage,
        provide_context=True,
        dag=dag
    )
	
    upload_file_to_stage_task = PythonOperator(
        task_id='upload_file_to_stage',
        python_callable=upload_file_to_stage,
        provide_context=True,
        dag=dag
    )
	
    write_to_raw_table_task = PythonOperator(
        task_id='write_to_raw_table',
        python_callable=write_to_raw_table,
        provide_context=True,
        dag=dag
    )
	
    write_to_stage_table_task = PythonOperator(
        task_id='write_to_stage_table',
        python_callable=write_to_stage_table,
        provide_context=True,
        dag=dag
    )
	
    write_to_master_table_task = PythonOperator(
        task_id='write_to_master_table',
        python_callable=write_to_master_table,
        provide_context=True,
        dag=dag
    )


    parse_csv_task >> create_data_streams_task >> create_stage_task >> upload_file_to_stage_task >> write_to_raw_table_task >> write_to_stage_table_task >> write_to_master_table_task

