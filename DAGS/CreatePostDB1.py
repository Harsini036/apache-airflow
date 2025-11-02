from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

default_args = {
    'owner': 'neo',
    'start_date': datetime(2025, 11, 2),
    'retries': 1
}

with DAG(
    dag_id='simple_postgres_dag_1',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='postgrelocal',
        sql="""
        CREATE TABLE IF NOT EXISTS airflow_db.users_airflow (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            email VARCHAR(50)
        );
        """
    )

    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data',
        conn_id='postgrelocal',
        sql="""
        INSERT INTO airflow_db.users_airflow (name, email) VALUES
        ('Neo', 'neo@example.com');
        """
    )

    create_table >> insert_data
