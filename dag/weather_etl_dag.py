import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configurez le chemin PYTHONPATH avant tous les autres imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

# Maintenant vous pouvez importer vos modules
from script.extract import process_historical_data
from script.transform import calculate_comfort_scores
from script.load import load_to_database

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_tourism_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['weather', 'tourism'],
) as dag:

    extract_task = PythonOperator(
        task_id='process_historical_data',
        python_callable=process_historical_data,
        op_kwargs={
            'input_dir': '/opt/airflow/data/raw/historical',
            'output_path': '/opt/airflow/data/processed/data.csv'
        }
    )

    transform_scores_task = PythonOperator(
        task_id='calculate_comfort_scores',
        python_callable=calculate_comfort_scores,
        op_kwargs={
            'input_path': '/opt/airflow/data/processed/data.csv',
            'output_path': '/opt/airflow/data/processed/scored_data.csv'
        }
    )

    transform_star_task = PythonOperator(
        task_id='create_star_schema',
        python_callable=create_star_schema,
        op_kwargs={
            'input_path': '/opt/airflow/data/processed/scored_data.csv',
            'output_dir': '/opt/airflow/data/star_schema'
        }
    )

    load_db_task = PythonOperator(
        task_id='load_to_database',
        python_callable=load_to_database,
        op_kwargs={
            'star_schema_dir': '/opt/airflow/data/star_schema',
            'db_path': '/opt/airflow/data/weather.db'
        }
    )

    recommend_task = PythonOperator(
        task_id='generate_recommendations',
        python_callable=generate_recommendations,
        op_kwargs={
            'input_path': '/opt/airflow/data/processed/scored_data.csv',
            'output_dir': '/opt/airflow/data/recommendations'
        }
    )

    extract_task >> transform_scores_task >> transform_star_task >> load_db_task
    transform_scores_task >> recommend_task