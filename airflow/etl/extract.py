import os
import pandas as pd
import logging
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)
DATA_FILE = os.getenv('WALMART_DATA_PATH', '/opt/airflow/data/walmart.csv')

def extract(execution_date=None, **kwargs):
    if not os.path.exists(DATA_FILE):
        raise AirflowException(f"File not found at {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y', errors='coerce')
    df = df.dropna(subset=['Date'])

    if df.empty:
        logger.warning(f"No data found for date {execution_date}. Skipping extract.")
        return pd.DataFrame().to_json(orient='records')

    logger.info(f"Found {len(df)} rows for date {execution_date}")
    return df.to_json(orient='records')
