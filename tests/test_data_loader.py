"""
Tests for data loader module.
"""
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime


class TestDataLoaderInitialization:
    """Tests for DataLoader initialization."""

    def test_initialization(self):
        """Test DataLoader initialization."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        loader = DataLoader(mock_client)

        assert loader.bq_client == mock_client


class TestDownloadParquet:
    """Tests for download_parquet method."""

    @patch("src.data_loader.io.BytesIO")
    @patch("src.data_loader.pq.read_table")
    @patch("src.data_loader.requests.get")
    def test_download_parquet_success(self, mock_get, mock_read_table, mock_bytesio):
        """Test successful parquet file download."""
        from src.data_loader import DataLoader

        # Mock HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b"mock_parquet_data"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Mock BytesIO
        mock_bytesio_instance = Mock()
        mock_bytesio.return_value = mock_bytesio_instance

        # Mock PyArrow parquet read
        mock_df = pd.DataFrame(
            {
                "VendorID": [1, 2],
                "tpep_pickup_datetime": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
                "tpep_dropoff_datetime": [
                    datetime(2024, 1, 1, 0, 30),
                    datetime(2024, 1, 2, 0, 30),
                ],
                "trip_distance": [2.5, 3.0],
                "total_amount": [15.0, 18.0],
            }
        )
        mock_table = Mock()
        mock_table.to_pandas.return_value = mock_df
        mock_read_table.return_value = mock_table

        mock_client = Mock()
        loader = DataLoader(mock_client)

        result = loader.download_parquet(1)

        # Verify mocks were called
        assert mock_get.called
        assert mock_read_table.called

        # Should return DataFrame (not None)
        assert result is not None
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2

    @patch("src.data_loader.requests.get")
    def test_download_parquet_handles_http_error(self, mock_get):
        """Test download handles HTTP errors."""
        from src.data_loader import DataLoader
        import requests

        mock_get.side_effect = requests.exceptions.HTTPError("404 Not Found")

        mock_client = Mock()
        loader = DataLoader(mock_client)

        result = loader.download_parquet(1)

        assert result is None

    @patch("src.data_loader.requests.get")
    def test_download_parquet_handles_network_error(self, mock_get):
        """Test download handles network errors."""
        from src.data_loader import DataLoader
        import requests

        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")

        mock_client = Mock()
        loader = DataLoader(mock_client)

        result = loader.download_parquet(1)

        assert result is None


class TestCheckIfDataExists:
    """Tests for check_if_data_exists_in_staging method."""

    def test_data_does_not_exist(self):
        """Test checking when data doesn't exist."""
        from src.data_loader import DataLoader

        mock_df = pd.DataFrame(
            {
                "tpep_pickup_datetime": [datetime(2024, 1, 1, 10, 0)],
                "tpep_dropoff_datetime": [datetime(2024, 1, 1, 10, 30)],
                "VendorID": [1],
                "trip_distance": [2.5],
                "total_amount": [15.0],
            }
        )

        mock_client = Mock()
        mock_client.execute_query.return_value = [Mock(count=0)]

        loader = DataLoader(mock_client)
        result = loader.check_if_data_exists_in_staging(mock_df)

        assert result is False

    def test_data_already_exists(self):
        """Test checking when data already exists."""
        from src.data_loader import DataLoader

        mock_df = pd.DataFrame(
            {
                "tpep_pickup_datetime": [
                    datetime(2024, 1, 1, 0, 0),
                    datetime(2024, 1, 31, 23, 59),
                ],
                "tpep_dropoff_datetime": [
                    datetime(2024, 1, 1, 0, 30),
                    datetime(2024, 2, 1, 0, 29),
                ],
                "VendorID": [1, 2],
                "trip_distance": [2.5, 3.0],
                "total_amount": [15.0, 18.0],
            }
        )

        mock_client = Mock()
        mock_client.execute_query.side_effect = [[Mock(count=1)], [Mock(count=1)]]

        loader = DataLoader(mock_client)
        result = loader.check_if_data_exists_in_staging(mock_df)

        assert result is True


class TestLoadToStaging:
    """Tests for load_to_staging method."""

    def test_load_to_staging_success(self):
        """Test loading DataFrame to staging."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        mock_client.load_dataframe_to_table.return_value = 100

        loader = DataLoader(mock_client)

        df = pd.DataFrame(
            {
                "VendorID": [1, 2],
                "tpep_pickup_datetime": [datetime(2024, 1, 1), datetime(2024, 1, 2)],
                "trip_distance": [2.5, 3.0],
            }
        )

        rows_loaded = loader.load_to_staging(df)

        mock_client.load_dataframe_to_table.assert_called_once()
        assert rows_loaded == 100


class TestShouldLoadToRaw:
    """Tests for should_load_to_raw method."""

    @patch("src.data_loader.DataLoader.get_raw_row_count")
    @patch("src.data_loader.DataLoader.get_staging_row_count_2024")
    def test_should_load_when_counts_differ(self, mock_staging_count, mock_raw_count):
        """Test should load when counts differ."""
        from src.data_loader import DataLoader

        mock_staging_count.return_value = 1000000
        mock_raw_count.return_value = 500000

        mock_client = Mock()
        loader = DataLoader(mock_client)

        result = loader.should_load_to_raw()

        assert result is True

    @patch("src.data_loader.DataLoader.get_raw_row_count")
    @patch("src.data_loader.DataLoader.get_staging_row_count_2024")
    def test_should_not_load_when_counts_match(
        self, mock_staging_count, mock_raw_count
    ):
        """Test should not load when counts match."""
        from src.data_loader import DataLoader

        mock_staging_count.return_value = 1000000
        mock_raw_count.return_value = 1000000

        mock_client = Mock()
        loader = DataLoader(mock_client)

        result = loader.should_load_to_raw()

        assert result is False


class TestLoadIncrementalToStaging:
    """Tests for load_incremental_to_staging method."""

    @patch("src.data_loader.DataLoader.load_to_staging")
    @patch("src.data_loader.DataLoader.check_if_data_exists_in_staging")
    @patch("src.data_loader.DataLoader.download_parquet")
    def test_load_incremental_new_data(
        self, mock_download, mock_check_exists, mock_load_staging
    ):
        """Test loading new month data."""
        from src.data_loader import DataLoader

        mock_df = pd.DataFrame(
            {
                "VendorID": [1] * 100,
                "tpep_pickup_datetime": [datetime(2024, 1, 1)] * 100,
                "tpep_dropoff_datetime": [datetime(2024, 1, 1, 0, 30)] * 100,
                "trip_distance": [2.5] * 100,
                "total_amount": [15.0] * 100,
            }
        )
        mock_download.return_value = mock_df
        mock_check_exists.return_value = False
        mock_load_staging.return_value = 100

        mock_client = Mock()
        loader = DataLoader(mock_client)

        rows_loaded = loader.load_incremental_to_staging(1)

        mock_download.assert_called_once_with(1)
        mock_load_staging.assert_called_once()
        assert rows_loaded == 100

    @patch("src.data_loader.DataLoader.check_if_data_exists_in_staging")
    @patch("src.data_loader.DataLoader.download_parquet")
    def test_load_incremental_data_exists(self, mock_download, mock_check_exists):
        """Test when data already exists (skip load)."""
        from src.data_loader import DataLoader

        mock_df = pd.DataFrame(
            {
                "VendorID": [1] * 100,
                "tpep_pickup_datetime": [datetime(2024, 1, 1)] * 100,
                "tpep_dropoff_datetime": [datetime(2024, 1, 1, 0, 30)] * 100,
                "trip_distance": [2.5] * 100,
                "total_amount": [15.0] * 100,
            }
        )
        mock_download.return_value = mock_df
        mock_check_exists.return_value = True

        mock_client = Mock()
        loader = DataLoader(mock_client)

        rows_loaded = loader.load_incremental_to_staging(1)

        assert rows_loaded == 0


class TestDownloadAllMonths:
    """Tests for download_all_months method."""

    @patch("src.data_loader.DataLoader.load_to_staging")
    @patch("src.data_loader.DataLoader.check_if_data_exists_in_staging")
    @patch("src.data_loader.DataLoader.download_parquet")
    def test_download_all_months(
        self, mock_download, mock_check_exists, mock_load_staging
    ):
        """Test downloading all 12 months."""
        from src.data_loader import DataLoader

        def create_mock_df(month):
            return pd.DataFrame(
                {
                    "VendorID": [1] * 1000000,
                    "tpep_pickup_datetime": [datetime(2024, month, 1)] * 1000000,
                    "tpep_dropoff_datetime": [datetime(2024, month, 1, 0, 30)]
                    * 1000000,
                    "trip_distance": [2.5] * 1000000,
                    "total_amount": [15.0] * 1000000,
                }
            )

        mock_download.side_effect = [create_mock_df(i) for i in range(1, 13)]
        mock_check_exists.return_value = False
        mock_load_staging.return_value = 1000000

        mock_client = Mock()
        loader = DataLoader(mock_client)

        total_rows = loader.download_all_months()

        assert mock_download.call_count == 12
        assert mock_load_staging.call_count == 12
        assert total_rows == 12000000

    @patch("src.data_loader.DataLoader.load_to_staging")
    @patch("src.data_loader.DataLoader.check_if_data_exists_in_staging")
    @patch("src.data_loader.DataLoader.download_parquet")
    def test_download_all_months_some_skipped(
        self, mock_download, mock_check_exists, mock_load_staging
    ):
        """Test when some months already exist."""
        from src.data_loader import DataLoader

        def create_mock_df(month):
            return pd.DataFrame(
                {
                    "VendorID": [1] * 1000000,
                    "tpep_pickup_datetime": [datetime(2024, month, 1)] * 1000000,
                    "tpep_dropoff_datetime": [datetime(2024, month, 1, 0, 30)]
                    * 1000000,
                    "trip_distance": [2.5] * 1000000,
                    "total_amount": [15.0] * 1000000,
                }
            )

        mock_download.side_effect = [create_mock_df(i) for i in range(1, 13)]
        check_results = [True, True, True] + [False] * 9
        mock_check_exists.side_effect = check_results
        mock_load_staging.return_value = 1000000

        mock_client = Mock()
        loader = DataLoader(mock_client)

        total_rows = loader.download_all_months()

        assert mock_download.call_count == 12
        assert mock_check_exists.call_count == 12
        assert mock_load_staging.call_count == 9
        assert total_rows == 9000000


class TestGetRowCounts:
    """Tests for row count methods."""

    def test_get_staging_row_count_2024(self):
        """Test getting staging row count for 2024."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        mock_client.get_row_count.side_effect = [1000000, 950000]
        mock_client.execute_query.side_effect = [
            [Mock(null_count=0)],
            [Mock(min_dt=datetime(2024, 1, 1), max_dt=datetime(2024, 12, 31))],
        ]

        loader = DataLoader(mock_client)
        count = loader.get_staging_row_count_2024()

        assert count == 950000

    def test_get_raw_row_count(self):
        """Test getting raw row count."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        mock_client.get_row_count.return_value = 800000

        loader = DataLoader(mock_client)
        count = loader.get_raw_row_count()

        assert count == 800000


class TestIsStagingEmpty:
    """Tests for is_staging_empty method."""

    def test_staging_is_empty(self):
        """Test when staging is empty."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        mock_client.get_row_count.return_value = 0

        loader = DataLoader(mock_client)
        result = loader.is_staging_empty()

        assert result is True

    def test_staging_is_not_empty(self):
        """Test when staging has data."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        mock_client.get_row_count.return_value = 1000000

        loader = DataLoader(mock_client)
        result = loader.is_staging_empty()

        assert result is False


class TestIntegrationConcepts:
    """Test high-level integration concepts."""

    def test_data_loader_has_required_methods(self):
        """Test that DataLoader has all required methods."""
        from src.data_loader import DataLoader

        mock_client = Mock()
        loader = DataLoader(mock_client)

        assert hasattr(loader, "download_parquet")
        assert hasattr(loader, "load_incremental_to_staging")
        assert hasattr(loader, "load_to_staging")
        assert hasattr(loader, "check_if_data_exists_in_staging")
        assert hasattr(loader, "should_load_to_raw")
        assert hasattr(loader, "download_all_months")
        assert hasattr(loader, "is_staging_empty")
        assert hasattr(loader, "get_staging_row_count_2024")
        assert hasattr(loader, "get_raw_row_count")
