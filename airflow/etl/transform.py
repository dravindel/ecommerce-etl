import logging
import pandas as pd
from io import StringIO
from airflow.exceptions import AirflowException

logger = logging.getLogger(__name__)

def transform(ti=None, **kwargs):
    if not ti:
        raise AirflowException("TaskInstance is required for transform")

    df_json = ti.xcom_pull(task_ids='extract')
    if not df_json:
        raise AirflowException("No data to transform")

    df = pd.read_json(StringIO(df_json), orient='records', convert_dates=['Date'])
    if df.empty:
        logger.warning("Empty DataFrame received in transform")
        return df.to_json(orient='records')

    df['store_contribution_pct'] = (df['Weekly_Sales'] / df['Weekly_Sales'].sum()) * 100
    df['holiday_multiplier'] = 1 + 0.1 * df['Holiday_Flag']
    df['adjusted_sales'] = df['Weekly_Sales'] * df['holiday_multiplier']
    df['unemployment_impact'] = df['Weekly_Sales'] * (df['Unemployment'] / 100)
    df['holiday_impact'] = df['Holiday_Flag'] * df['adjusted_sales']
    df['sales_per_store'] = df['Weekly_Sales']

    logger.info("Transformation completed with new calculated fields")
    return df.to_json(orient='records')
