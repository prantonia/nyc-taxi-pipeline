"""
Shared pytest fixtures for all test modules.
Provides common mocks and test data.
"""
import pytest
from unittest.mock import MagicMock
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


@pytest.fixture
def mock_bigquery_client():
    """Shared BigQuery client mock"""
    mock = MagicMock()
    mock.execute_query = MagicMock()
    mock.execute_dml = MagicMock(return_value=1000)
    mock.execute_ddl = MagicMock()
    mock.execute_sql_file = MagicMock()
    mock.table_exists = MagicMock(return_value=True)
    mock.get_row_count = MagicMock(return_value=1000)
    mock.load_parquet_bytes = MagicMock(return_value=1000)
    mock.close = MagicMock()
    return mock


@pytest.fixture
def sample_parquet_data():
    """Sample parquet-like data for testing"""
    return {
        'VendorID': [1, 2, 1],
        'tpep_pickup_datetime': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'tpep_dropoff_datetime': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'passenger_count': [1, 2, 1],
        'trip_distance': [1.5, 2.3, 0.8],
        'fare_amount': [10.0, 15.5, 7.0],
        'total_amount': [12.0, 18.0, 9.0],
    }


@pytest.fixture
def mock_environment(monkeypatch):
    """Set up test environment variables"""
    monkeypatch.setenv('GCP_PROJECT_ID', 'test-project')
    monkeypatch.setenv('BQ_DATASET', 'test_dataset')
    monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', './test-credentials.json')
    monkeypatch.setenv('LOG_LEVEL', 'DEBUG')


@pytest.fixture(autouse=True)
def reset_logging():
    """Reset logging configuration after each test"""
    import logging
    logging.getLogger().handlers = []
    yield
    logging.getLogger().handlers = []
    