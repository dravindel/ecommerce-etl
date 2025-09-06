import pytest
import pandas as pd
from unittest.mock import MagicMock
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../airflow/dags/utils')))

from etl_functions import transform

@pytest.fixture
def mock_kwargs():
    df = pd.DataFrame({
        'Store': [1, 1],
        'Date': pd.to_datetime(['2025-09-03', '2025-09-05']),
        'Weekly_Sales': [1643690.90, 1641957.44],
        'Holiday_Flag': [0, 1],
        'Temperature': [42.31, 38.51],
        'Fuel_Price': [2.572, 2.548],
        'CPI': [211.096358, 211.242170],
        'Unemployment': [8.106, 8.106]
    })
    ti = MagicMock()
    ti.xcom_pull.return_value = df.to_json(orient='records')
    return {'ti': ti}

def test_transform(mock_kwargs):
    transformed_json = transform(**mock_kwargs)
    transformed = pd.read_json(transformed_json, orient='records')
    assert 'store_contribution_pct' in transformed.columns
    assert 'holiday_multiplier' in transformed.columns
    assert 'adjusted_sales' in transformed.columns
    assert 'unemployment_impact' in transformed.columns
    assert len(transformed) == 2
    assert transformed['store_contribution_pct'].iloc[0] > 0
    assert transformed['adjusted_sales'].iloc[1] == 1641957.44 * 1.1