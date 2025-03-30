from datetime import datetime 
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'wanyua',
    'start_date': datetime(2025, 3, 31, 8,00),
    'retries': 1,
}

def stream_data():
    import json
    import requests
    res = requests.get("https://randomuser.me/api/")
    res.json()


with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False
         ) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )