import os
import logging
import pandas as pd
import great_expectations as gx
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  
DATA_PATH = os.getenv("DATA_PATH", os.path.join(BASE_DIR, "data", "walmart.csv"))

if not os.path.exists(DATA_PATH):
    logger.error(f"Data file not found at {DATA_PATH}")
    raise FileNotFoundError(f"Data file not found at {DATA_PATH}")

df = pd.read_csv(DATA_PATH)
logger.info(f"Loaded {len(df)} rows from {DATA_PATH}")

context = gx.get_context()

datasource = context.sources.add_pandas(name="pandas_datasource")
asset = datasource.add_dataframe_asset(name="sales_data")

batch_request = asset.build_batch_request(dataframe=df)

expectation_suite_name = "sales_suite"
context.add_or_update_expectation_suite(expectation_suite_name)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name
)

validator.expect_column_values_to_not_be_null("Store")
validator.expect_column_values_to_be_between("Weekly_Sales", min_value=0, max_value=1e7)
validator.expect_column_to_exist("Date")
validator.expect_column_values_to_not_be_null("Weekly_Sales")

validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="sales_checkpoint",
    data_context=context,
    validations=[{"batch_request": batch_request, "expectation_suite_name": expectation_suite_name}],
)

results = checkpoint.run()

if not results["success"]:
    logger.error("Data quality check failed")
    raise AirflowException("Data quality check failed")

logger.info("Data quality passed!")
