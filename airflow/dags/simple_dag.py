from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from etl import extract, transform, load

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024,6,25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def scrape_data():
    print(".....EXTRACT....")
    extract()

def transform_data():
    print(".....TRANSFORM....")
    transform()

def load_data():
    print(".....LOAD....")
    load()

with DAG(
    'web_scraping_etl',
    default_args=default_args,
    description='A simple web scraping ETL DAG',
    schedule_interval=timedelta(days=1),
    
) as dag:

    t1 = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_data,
    )

    t2 = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    t3 = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    t1 >> t2 >> t3
