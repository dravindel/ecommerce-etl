import os
import pandas as pd
import logging
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)
DATA_FILE = os.getenv('WALMART_DATA_PATH', '/opt/airflow/data/walmart.csv')

def data_quality_check(**kwargs):
    if not os.path.exists(DATA_FILE):
        raise AirflowException(f"File not found at {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)
    df['Date'] = pd.to_datetime(df['Date'], format='%d-%m-%Y', errors='coerce')

    required_columns = ['Store', 'Weekly_Sales', 'Date']
    for col in required_columns:
        if col not in df.columns:
            raise AirflowException(f"Column {col} not found in the data")

    if df.empty:
        logger.warning("DataFrame is empty, skipping data quality checks")
        return

    if df['Store'].isnull().any():
        raise AirflowException("Null values found in Store column")
    if df['Weekly_Sales'].isnull().any():
        raise AirflowException("Null values found in Weekly_Sales column")

    logger.info("Data quality passed!")
