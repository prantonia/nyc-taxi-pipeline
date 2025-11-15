"""
Tests for metadata manager module.
"""
import pytest
from unittest.mock import Mock
from datetime import datetime

from src.metadata_manager import MetadataManager
from src.config import STATUS_SUCCESS, STATUS_FAILED, STATUS_SKIPPED


class TestMetadataManager:
    """Test cases for MetadataManager class."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mock BigQuery client."""
        mock_client = Mock()
        mock_client.execute_dml = Mock(return_value=1)
        mock_client.execute_query = Mock(return_value=[])
        return mock_client

    @pytest.fixture
    def metadata_manager(self, mock_bq_client):
        """Create a MetadataManager instance with mock client."""
        return MetadataManager(mock_bq_client)

    def test_initialization(self, mock_bq_client):
        """Test MetadataManager initialization."""
        manager = MetadataManager(mock_bq_client)
        assert manager.bq_client == mock_bq_client
        assert manager.table_id is not None

    def test_record_run_success(self, metadata_manager, mock_bq_client):
        """Test recording a successful pipeline run."""
        metadata_manager.record_run(
            pipeline_name="full_refresh",
            status=STATUS_SUCCESS,
            rows_loaded=1000000,
            month=None,
            runtime=120.5,
        )

        # Verify DML was executed
        assert mock_bq_client.execute_dml.called
        call_args = mock_bq_client.execute_dml.call_args[0][0]
        assert "INSERT INTO" in call_args
        assert "full_refresh" in call_args
        assert "SUCCESS" in call_args
        assert "1000000" in call_args

    def test_record_run_failed(self, metadata_manager, mock_bq_client):
        """Test recording a failed pipeline run."""
        metadata_manager.record_run(
            pipeline_name="incremental",
            status=STATUS_FAILED,
            rows_loaded=0,
            month=3,
            runtime=45.2,
            error_message="Connection timeout",
        )

        assert mock_bq_client.execute_dml.called
        call_args = mock_bq_client.execute_dml.call_args[0][0]
        assert "FAILED" in call_args
        assert "Connection timeout" in call_args

    def test_record_run_skipped(self, metadata_manager, mock_bq_client):
        """Test recording a skipped pipeline run."""
        metadata_manager.record_run(
            pipeline_name="full_refresh",
            status=STATUS_SKIPPED,
            rows_loaded=0,
            month=None,
            runtime=5.0,
        )

        assert mock_bq_client.execute_dml.called
        call_args = mock_bq_client.execute_dml.call_args[0][0]
        assert "SKIPPED" in call_args
        assert "full year" in call_args

    def test_get_last_successful_run_exists(self, metadata_manager, mock_bq_client):
        """Test getting last successful run when it exists."""
        # Mock query result
        mock_row = Mock()
        mock_row.pipeline_name = "full_refresh"
        mock_row.date_range = "2024-01 - 2024-12"
        mock_row.month_loaded = "full year"
        mock_row.status = STATUS_SUCCESS
        mock_row.rows_loaded = 1000000
        mock_row.run_timestamp = datetime.now()
        mock_row.runtime = 120.5
        mock_row.error_message = ""

        mock_bq_client.execute_query.return_value = [mock_row]

        result = metadata_manager.get_last_successful_run("full_refresh")

        assert result is not None
        assert result["pipeline_name"] == "full_refresh"
        assert result["status"] == STATUS_SUCCESS
        assert result["rows_loaded"] == 1000000

    def test_get_last_successful_run_not_exists(self, metadata_manager, mock_bq_client):
        """Test getting last successful run when none exists."""
        mock_bq_client.execute_query.return_value = []

        result = metadata_manager.get_last_successful_run("full_refresh")

        assert result is None

    def test_get_last_successful_month(self, metadata_manager, mock_bq_client):
        """Test getting last successfully loaded month."""
        mock_row = Mock()
        mock_row.month_loaded = "March"

        mock_bq_client.execute_query.return_value = [mock_row]

        result = metadata_manager.get_last_successful_month()

        assert result == 3

    def test_get_last_successful_month_none(self, metadata_manager, mock_bq_client):
        """Test getting last month when none loaded."""
        mock_bq_client.execute_query.return_value = []

        result = metadata_manager.get_last_successful_month()

        assert result is None

    def test_is_full_year_loaded_true_full_refresh(
        self, metadata_manager, mock_bq_client
    ):
        """Test checking if full year loaded via full refresh."""
        mock_row = Mock()
        mock_row.count = 1

        mock_bq_client.execute_query.return_value = [mock_row]

        result = metadata_manager.is_full_year_loaded()

        assert result is True

    def test_is_full_year_loaded_true_incremental(
        self, metadata_manager, mock_bq_client
    ):
        """Test checking if full year loaded via incremental (all 12 months)."""
        # First query returns 0 (no full refresh)
        # Second query returns 12 (all months via incremental)
        mock_row_0 = Mock()
        mock_row_0.count = 0

        mock_row_12 = Mock()
        mock_row_12.count = 12

        mock_bq_client.execute_query.side_effect = [
            [mock_row_0],  # Full refresh check
            [mock_row_12],  # Incremental check
        ]

        result = metadata_manager.is_full_year_loaded()

        assert result is True

    def test_is_full_year_loaded_false(self, metadata_manager, mock_bq_client):
        """Test checking if full year loaded when it's not."""
        mock_row = Mock()
        mock_row.count = 0

        mock_bq_client.execute_query.return_value = [mock_row]

        result = metadata_manager.is_full_year_loaded()

        assert result is False

    def test_get_loaded_months(self, metadata_manager, mock_bq_client):
        """Test getting list of loaded months."""
        mock_rows = [
            Mock(month_loaded="January"),
            Mock(month_loaded="February"),
            Mock(month_loaded="March"),
        ]

        mock_bq_client.execute_query.return_value = mock_rows

        result = metadata_manager.get_loaded_months()

        assert result == [1, 2, 3]

    def test_get_run_history(self, metadata_manager, mock_bq_client):
        """Test getting run history."""
        mock_row = Mock()
        mock_row.pipeline_name = "full_refresh"
        mock_row.date_range = "2024-01 - 2024-12"
        mock_row.month_loaded = "full year"
        mock_row.status = STATUS_SUCCESS
        mock_row.rows_loaded = 1000000
        mock_row.run_timestamp = datetime.now()
        mock_row.runtime = 120.5
        mock_row.error_message = ""

        mock_bq_client.execute_query.return_value = [mock_row]

        result = metadata_manager.get_run_history(limit=10)

        assert len(result) == 1
        assert result[0]["pipeline_name"] == "full_refresh"
        assert result[0]["status"] == STATUS_SUCCESS


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
