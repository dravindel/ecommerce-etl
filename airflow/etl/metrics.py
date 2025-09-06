import logging
from sqlalchemy import text
from .load import get_engine 

logger = logging.getLogger(__name__)

def calculate_metrics(**kwargs):
    engine = get_engine()

    sql_file_path = "/opt/airflow/sql/metrics.sql"
    
    try:
        with open(sql_file_path, "r") as f:
            sql_query = f.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found at {sql_file_path}")
    
    with engine.begin() as conn:
        conn.execute(text(sql_query))
    
    logger.info("Metrics calculated and updated successfully")
