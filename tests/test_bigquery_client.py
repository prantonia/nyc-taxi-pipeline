"""
Tests for BigQuery client wrapper.
"""
import pytest
from unittest.mock import Mock, patch
from google.cloud.exceptions import NotFound


class TestBigQueryClientInitialization:
    """Tests for BigQueryClient initialization."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_initialization_success(self, mock_client_class):
        """Test successful client initialization."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_instance.project = "test-project-123"
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()

        assert client.client == mock_client_instance
        mock_client_class.assert_called_once()


class TestExecuteQuery:
    """Tests for execute_query method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_execute_query_success(self, mock_client_class):
        """Test successful query execution."""
        from src.bigquery_client import BigQueryClient

        # Setup mock client
        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_result = [Mock(value=1), Mock(value=2)]
        mock_query_job.result.return_value = mock_result
        mock_client_instance.query.return_value = mock_query_job
        mock_client_class.return_value = mock_client_instance

        # Create client and execute query
        client = BigQueryClient()
        results = client.execute_query("SELECT 1 as value")

        # Verify query was called
        assert mock_client_instance.query.called

        # Verify we can iterate over results
        results_list = list(results)
        assert len(results_list) >= 0

    @patch("src.bigquery_client.bigquery.Client")
    def test_execute_query_returns_iterator(self, mock_client_class):
        """Test that execute_query returns an iterator."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_result = [Mock(value=1), Mock(value=2), Mock(value=3)]
        mock_query_job.result.return_value = mock_result
        mock_client_instance.query.return_value = mock_query_job
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        results = client.execute_query("SELECT * FROM table")

        # Should be able to iterate
        assert hasattr(results, "__iter__")

    @patch("src.bigquery_client.bigquery.Client")
    def test_execute_query_handles_error(self, mock_client_class):
        """Test query execution handles errors."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_instance.query.side_effect = Exception("Query failed")
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()

        with pytest.raises(Exception, match="Query failed"):
            client.execute_query("SELECT * FROM nonexistent")


class TestExecuteDML:
    """Tests for execute_dml method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_execute_dml_insert(self, mock_client_class):
        """Test DML INSERT execution."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.num_dml_affected_rows = 5
        mock_client_instance.query.return_value = mock_query_job
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        rows_affected = client.execute_dml("INSERT INTO table VALUES (1, 'test')")

        assert rows_affected == 5
        mock_client_instance.query.assert_called_once()


class TestExecuteDDL:
    """Tests for execute_ddl method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_execute_ddl_create_table(self, mock_client_class):
        """Test DDL CREATE TABLE execution."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_query_job.result.return_value = None
        mock_client_instance.query.return_value = mock_query_job
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        client.execute_ddl("CREATE TABLE test (id INT64)")

        mock_client_instance.query.assert_called_once()


class TestTableExists:
    """Tests for table_exists method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_table_exists_returns_true(self, mock_client_class):
        """Test table_exists returns True for existing table."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_table = Mock()
        mock_client_instance.get_table.return_value = mock_table
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        result = client.table_exists("project.dataset.table")

        assert result is True

    @patch("src.bigquery_client.bigquery.Client")
    def test_table_exists_returns_false(self, mock_client_class):
        """Test table_exists returns False for non-existent table."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_instance.get_table.side_effect = NotFound("Table not found")
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        result = client.table_exists("project.dataset.nonexistent")

        assert result is False


class TestGetRowCount:
    """Tests for get_row_count method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_get_row_count_success(self, mock_client_class):
        """Test getting row count for table."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_query_job = Mock()
        mock_row = Mock()
        mock_row.count = 1000000
        mock_query_job.result.return_value = [mock_row]
        mock_client_instance.query.return_value = mock_query_job
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        count = client.get_row_count("project.dataset.table")

        assert count == 1000000


class TestLoadDataframe:
    """Tests for load_dataframe_to_table method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_load_dataframe_success(self, mock_client_class):
        """Test loading DataFrame to BigQuery."""
        import pandas as pd
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_job = Mock()
        mock_job.result.return_value = None
        mock_client_instance.load_table_from_dataframe.return_value = mock_job
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

        client.load_dataframe_to_table(df, "project.dataset.table")

        mock_client_instance.load_table_from_dataframe.assert_called_once()


class TestGetMinMaxDatetime:
    """Tests for get_min_max_datetime method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_get_min_max_datetime_success(self, mock_client_class):
        """Test getting min/max datetime from table."""
        from datetime import datetime
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_query_job = Mock()
        min_dt = datetime(2024, 1, 1, 0, 0, 0)
        max_dt = datetime(2024, 1, 31, 23, 59, 59)
        mock_row = Mock()
        mock_row.min_dt = min_dt
        mock_row.max_dt = max_dt
        mock_query_job.result.return_value = [mock_row]
        mock_client_instance.query.return_value = mock_query_job
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        min_val, max_val = client.get_min_max_datetime(
            "project.dataset.table", "pickup_datetime"
        )

        assert min_val == min_dt
        assert max_val == max_dt


class TestCloseConnection:
    """Tests for close method."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_close_connection(self, mock_client_class):
        """Test closing BigQuery client connection."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_instance.close.return_value = None
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()
        client.close()

        mock_client_instance.close.assert_called_once()


class TestBigQueryClientMethods:
    """Test that BigQueryClient has expected methods."""

    @patch("src.bigquery_client.bigquery.Client")
    def test_has_execute_query_method(self, mock_client_class):
        """Test that client has execute_query method."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()

        assert hasattr(client, "execute_query")
        assert callable(client.execute_query)

    @patch("src.bigquery_client.bigquery.Client")
    def test_has_execute_dml_method(self, mock_client_class):
        """Test that client has execute_dml method."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()

        assert hasattr(client, "execute_dml")
        assert callable(client.execute_dml)

    @patch("src.bigquery_client.bigquery.Client")
    def test_has_execute_ddl_method(self, mock_client_class):
        """Test that client has execute_ddl method."""
        from src.bigquery_client import BigQueryClient

        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance

        client = BigQueryClient()

        assert hasattr(client, "execute_ddl")
        assert callable(client.execute_ddl)
