# Test script for Airflow DAG
# This script defines a simple Airflow DAG that prints "Hello Airflow!" to the console.

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('test_dag',
         start_date=datetime(2023, 1, 1),
         schedule_interval='@daily',
         catchup=False) as dag:

    task1 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow!"'
    )