from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from script.extract1 import extract_meteo
from script.merge2 import merge_files
from script.transform3 import transform_to_star

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 30),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}

CITIES = ['Paris', 'London', 'Tokyo', 'New York', 'Dubai', 'Sydney']

with DAG(
    'weather_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'tourism'],
) as dag:
    
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_kwargs={
                'city': city,
                'api_key': Variable.get("API_KEY"),
                'date': '{{ ds }}'
            },
            retries=3,
        )
        for city in CITIES
    ]
    
    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_kwargs={'date': '{{ ds }}'},
    )
    
    transform_task = PythonOperator(
        task_id='transform_to_star',
        python_callable=transform_to_star,
        op_kwargs={'date': '{{ ds }}'},
    )
    
    
    # Documentation
    extract_tasks[0].doc_md = """Extract weather data from OpenWeather API for each city"""
    merge_task.doc_md = """Merge all city data into a single file"""
    transform_task.doc_md = """Transform data into star schema"""
 
    
    # Orchestration
    extract_tasks >> merge_task >> transform_task 