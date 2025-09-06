from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract import extract
from etl.transform import transform
from etl.load import load_raw, load_processed
from etl.quality import data_quality_check
from etl.metrics import calculate_metrics

DEFAULT_ARGS = {
    'owner': 'dravindel',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='walmart_sales_etl',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2010, 2, 5),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=['walmart', 'etl'],
    description='ETL pipeline for Walmart sales data',
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
        op_kwargs={'execution_date': '{{ ds }}'}
    )

    quality_task = PythonOperator(
        task_id='data_quality',
        python_callable=data_quality_check,
    )

    load_raw_task = PythonOperator(
        task_id='load_raw',
        python_callable=load_raw,
        op_kwargs={'df_json': '{{ ti.xcom_pull(task_ids="extract") }}'}
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        op_kwargs={'df_json': '{{ ti.xcom_pull(task_ids="extract") }}'}
    )

    load_processed_task = PythonOperator(
        task_id='load_processed',
        python_callable=load_processed,
        op_kwargs={'df_json': '{{ ti.xcom_pull(task_ids="transform") }}'}
    )

    metrics_task = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics
    )

    extract_task >> quality_task >> load_raw_task
    load_raw_task >> transform_task >> load_processed_task >> metrics_task
