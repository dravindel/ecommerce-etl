import logging
import pandas as pd
from sqlalchemy import create_engine
from airflow.exceptions import AirflowException
import great_expectations as gx
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract(**kwargs):
    file_path = os.path.join('/opt/airflow/data', 'walmart.csv')
    logger.info(f"Checking file at: {file_path}")
    if not os.path.exists(file_path):
        logger.error(f"File not found at {file_path}")
        raise AirflowException("Walmart data file not found")
    df = pd.read_csv(file_path, parse_dates=['Date'])
    execution_date = kwargs['execution_date'].date()
    logger.info(f"Filtering data for date: {execution_date}")
    df = df[df['Date'].dt.date == execution_date]
    if df.empty:
        logger.warning(f"No data found for date {execution_date}")
        raise AirflowException(f"No data found for date {execution_date}")
    return df.to_json(orient='records')

def transform(**kwargs):
    df_json = kwargs['ti'].xcom_pull(task_ids='extract')
    if not df_json:
        raise AirflowException("No data to transform")
    df = pd.read_json(df_json, orient='records', convert_dates=['Date'])
    df['store_contribution_pct'] = (df['Weekly_Sales'] / df['Weekly_Sales'].sum()) * 100
    df['holiday_multiplier'] = 1 + 0.1 * df['Holiday_Flag']
    df['adjusted_sales'] = df['Weekly_Sales'] * df['holiday_multiplier']
    df['unemployment_impact'] = df['Weekly_Sales'] * (df['Unemployment'] / 100)
    return df.to_json(orient='records')

def load(**kwargs):
    df_json = kwargs['ti'].xcom_pull(task_ids='transform')
    if not df_json:
        raise AirflowException("No data to load")
    df = pd.read_json(df_json, orient='records', convert_dates=['Date'])
    engine = create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'))
    df.to_sql('processed_walmart', engine, if_exists='append', index=False, schema='public')

def calculate_metrics(**kwargs):
    engine = create_engine(os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'))
    query = """
    INSERT INTO metrics (date, total_sales, avg_sales_per_store, holiday_sales_increase)
    SELECT 
        "Date" AS date,
        SUM("Weekly_Sales") AS total_sales,
        AVG("Weekly_Sales") / (SELECT COUNT(DISTINCT "Store") FROM processed_walmart) AS avg_sales_per_store,
        SUM("adjusted_sales") - SUM("Weekly_Sales") AS holiday_sales_increase
    FROM processed_walmart
    GROUP BY "Date"
    ON CONFLICT ("date") DO UPDATE
    SET total_sales = EXCLUDED.total_sales,
        avg_sales_per_store = EXCLUDED.avg_sales_per_store,
        holiday_sales_increase = EXCLUDED.holiday_sales_increase;
    """
    with engine.connect() as conn:
        conn.execute(query)
        conn.commit()

def data_quality_check(**kwargs):
    context = gx.get_context()
    df = pd.read_csv(os.path.join('/opt/airflow/data', 'walmart.csv'))
    datasource = context.sources.add_pandas(name="pandas_datasource")
    asset = datasource.add_dataframe_asset(name="sales_data")
    batch_request = asset.build_batch_request(dataframe=df)
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name="sales_suite")
    validator.expect_column_values_to_not_be_null("Store")
    validator.expect_column_values_to_be_between("Weekly_Sales", min_value=0, max_value=1e7)
    validator.expect_column_to_exist("Date")
    validator.expect_column_values_to_not_be_null("Weekly_Sales")
    results = validator.validate()
    if not results["success"]:
        logger.error(f"Data quality check failed: {results['results']}")
        raise AirflowException("Data quality check failed")