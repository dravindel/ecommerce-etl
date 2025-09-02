from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os

DEFAULT_ARGS = {
    'owner': 'dravindel',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def extract(**kwargs):
    file_path = '/opt/airflow/data/walmart.csv'
    if not os.path.exists(file_path):
        raise FileNotFoundError("Walmart data file not found")
    df = pd.read_csv(file_path, parse_dates=['Date'])
    execution_date = kwargs['execution_date'].date()
    df = df[df['Date'] == execution_date]
    return df.to_json(orient='records')

def transform(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(task_ids='extract'), orient='records')
    if df.empty:
        return None
    df['sales_per_store'] = df['weekly_sales'] / df['store'].nunique() 
    df['holiday_impact'] = df['weekly_sales'] * df['holiday_flag'] 
    return df.to_json(orient='records')

def load(**kwargs):
    df = pd.read_json(kwargs['ti'].xcom_pull(task_ids='transform'), orient='records')
    if df is not None:
        engine = create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'))
        df.to_sql('processed_walmart', engine, if_exists='append', index=False)

def calculate_metrics(**kwargs):
    engine = create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'))
    query = """
    INSERT INTO metrics (date, total_sales, avg_sales_per_store, holiday_sales_increase)
    SELECT 
        date,
        SUM(weekly_sales) AS total_sales,
        AVG(sales_per_store) AS avg_sales_per_store,
        AVG(holiday_impact) AS holiday_sales_increase
    FROM processed_walmart
    GROUP BY date;
    """
    with engine.connect() as conn:
        conn.execute(query)

with DAG(
    'walmart_sales_etl',
    default_args=DEFAULT_ARGS,
    start_date=datetime(2010, 2, 5), 
    schedule_interval='@weekly',
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract, provide_context=True)
    transform_task = PythonOperator(task_id='transform', python_callable=transform, provide_context=True)
    load_task = PythonOperator(task_id='load', python_callable=load, provide_context=True)
    metrics_task = PythonOperator(task_id='calculate_metrics', python_callable=calculate_metrics)

    extract_task >> transform_task >> load_task >> metrics_task