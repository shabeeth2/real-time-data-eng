from  datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 23, 3, 00),
}

def stream_data():
    import json
    import requests

    res=requests.get('https://randomuser.me/api/')
    data=res.json()
    print(data)

with DAG('user automaation',
          default_args=default_args,
          schedule_interval='@daily',
          catchup=False) as dag:
    
    streaming = PythonOperator(
        task_id='streaming',
        python_callable=stream_data,
        dag=dag
    )
stream_data()