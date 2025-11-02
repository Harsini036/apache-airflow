from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def fetch_employees():
    hook = PostgresHook(postgres_conn_id="postgrelocal")
    records = hook.get_records("SELECT * FROM employees;")
    for row in records:
        print(f"Employee Record: {row}")

with DAG(
    dag_id="fetch_postgres_data",
    start_date=datetime(2025, 11, 1),
    schedule=None,  # manual trigger
    catchup=False,
    tags=["example"],
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_employees_task",
        python_callable=fetch_employees
    )
