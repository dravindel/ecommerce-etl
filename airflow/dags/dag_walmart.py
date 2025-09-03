from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from utils.etl_functions import extract, transform, load, calculate_metrics, data_quality_check

DEFAULT_ARGS = {
    'owner': 'dravindel',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your_email@example.com'],
}

with DAG(
    'walmart_sales_etl',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 9, 3, 16, 30), 
    schedule_interval='@hourly',
    catchup=False,
    max_active_runs=1,
    tags=['walmart', 'etl'],
    description='ETL pipeline for Walmart sales data',
) as dag:
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        provide_context=True
    )
    data_quality_task = PythonOperator(
        task_id='data_quality',
        python_callable=data_quality_check,
        provide_context=True
    )
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True
    )
    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True
    )
    metrics_task = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics,
        provide_context=True
    )

    extract_task >> data_quality_task >> transform_task >> load_task >> metrics_task