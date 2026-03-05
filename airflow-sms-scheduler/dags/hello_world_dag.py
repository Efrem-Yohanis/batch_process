"""
Ultra Simple Hello World DAG - Runs every 5 minutes
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def say_hello():
    print(f"👋 Hello World! Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return "Hello sent!"

with DAG(
    dag_id='simple_hello_world',
    start_date=datetime(2024, 1, 1),
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['hello'],
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=say_hello
    )