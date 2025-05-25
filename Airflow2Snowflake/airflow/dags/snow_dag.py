from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
#from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('create_insert_table',
        default_args=default_args,
        schedule = '@daily',
        catchup=False) as dag:

        create_table = SQLExecuteQueryOperator(
            task_id = "create_snowflake_table",
            conn_id = 'conn_id_snowflake',
            sql = '''CREATE TABLE IF NOT EXISTS student_info(
                student_id INTEGER NOT NULL,
                fname VARCHAR(255) NOT NULL,
                lname VARCHAR(255) NOT NULL,
                email VARCHAR(300) NOT NULL UNIQUE,
                subject VARCHAR(255) NOT NULL,
                score NUMERIC NOT NULL DEFAULT 0	
            )
            '''
        )

        data_insert = SQLExecuteQueryOperator(
            task_id = "insert_into_snowflake",
            conn_id = 'conn_id_snowflake',
            sql = '''INSERT INTO student_info(student_id, fname, lname, email, subject, score)
                    VALUES (1, 'John', 'James', 'john.james@schooldomain.com', 'Mathematics', 95),
                        (2, 'Abraham', 'Scott', 'abraham.scott@schooldomain.com', 'Biology',86),
                        (3, 'Jude', 'Titus', 'jude.titus@schooldomain.com', 'Mathematics', 89)
                 '''

        )
        
        create_table >> data_insert