import great_expectations as gx
from great_expectations.checkpoint import SimpleCheckpoint

context = gx.get_context()
datasource = context.sources.add_pandas(name="pandas_datasource")
asset = datasource.add_dataframe_asset(name="sales_data")
batch_request = asset.build_batch_request(dataframe=pd.read_csv('data/online_retail.csv'))

expectation_suite = context.add_or_update_expectation_suite("sales_suite")
expectation = expectation_suite.expect_column_values_to_not_be_null(column="CustomerID")
expectation_suite.add_expectation(expectation)  

checkpoint = SimpleCheckpoint(
    name="sales_checkpoint",
    context=context,
    validations=[{"batch_request": batch_request, "expectation_suite_name": "sales_suite"}],
)
results = checkpoint.run()
if not results["success"]:
    raise ValueError("Data quality check failed")