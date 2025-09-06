import logging
import os
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Numeric, Date
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

def get_engine():
    try:
        conn = BaseHook.get_connection("postgres_walmart")
        db_url = conn.get_uri()
    except Exception:
        db_url = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
        if not db_url:
            raise AirflowException("Database connection not found")

    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql+psycopg2://", 1)

    return create_engine(db_url)

def load_raw(ti=None, **kwargs):
    if not ti:
        raise AirflowException("TaskInstance required")

    df_json = ti.xcom_pull(task_ids='extract')
    if not df_json:
        raise AirflowException("No data to load")

    df = pd.read_json(StringIO(df_json), orient='records', convert_dates=['Date'])
    if df.empty:
        logger.warning("Empty DataFrame in load_raw")
        return

    df = df.rename(columns={
        'Store': 'store', 'Date': 'date', 'Weekly_Sales': 'weekly_sales',
        'Holiday_Flag': 'holiday_flag', 'Temperature': 'temperature', 'Fuel_Price': 'fuel_price',
        'CPI': 'cpi', 'Unemployment': 'unemployment'
    })

    engine = get_engine()
    df.to_sql(
        'raw_walmart', engine, if_exists='append', index=False, schema='public',
        dtype={'store': Integer, 'date': Date, 'weekly_sales': Numeric,
               'holiday_flag': Integer, 'temperature': Numeric, 'fuel_price': Numeric,
               'cpi': Numeric, 'unemployment': Numeric}
    )
    logger.info(f"Loaded {len(df)} rows into raw_walmart")

def load_processed(ti=None, **kwargs):
    """Загрузка трансформированных данных в таблицу processed_walmart"""
    if not ti:
        raise AirflowException("TaskInstance is required for load_processed")

    df_json = ti.xcom_pull(task_ids='transform')
    if not df_json:
        raise AirflowException("No data to load in processed_walmart")

    df = pd.read_json(StringIO(df_json), orient='records', convert_dates=['Date'])
    if df.empty:
        logger.warning("Empty DataFrame received in load_processed. Skipping.")
        return

    df = df.rename(columns={
        'Store': 'store', 'Date': 'date', 'Weekly_Sales': 'weekly_sales',
        'Holiday_Flag': 'holiday_flag', 'Temperature': 'temperature', 'Fuel_Price': 'fuel_price',
        'CPI': 'cpi', 'Unemployment': 'unemployment',
        'Sales_per_store': 'sales_per_store', 'Holiday_impact': 'holiday_impact',
        'Store_contribution_pct': 'store_contribution_pct', 'Holiday_multiplier': 'holiday_multiplier',
        'Adjusted_sales': 'adjusted_sales', 'Unemployment_impact': 'unemployment_impact'
    })

    engine = get_engine()
    df.to_sql(
        'processed_walmart', engine, if_exists='append', index=False, schema='public'
    )
    logger.info(f"Loaded {len(df)} rows into processed_walmart")
