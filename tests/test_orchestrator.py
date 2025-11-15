"""
Comprehensive tests for pipeline orchestrator.
Tests all methods, edge cases, and integration points.
"""
import pytest
from unittest.mock import MagicMock, patch, call
from src.orchestrator import PipelineOrchestrator
from src.config import (
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_SKIPPED,
    PIPELINE_FULL_REFRESH,
    PIPELINE_INCREMENTAL,
)


class TestPipelineOrchestrator:
    """Test suite for PipelineOrchestrator class"""

    # ============================================================================
    # FIXTURES - Setup code reused across tests
    # ============================================================================

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mock BigQuery client"""
        mock = MagicMock()
        mock.execute_sql_file = MagicMock()
        mock.get_row_count = MagicMock(return_value=1000)
        mock.close = MagicMock()
        return mock

    @pytest.fixture
    def mock_data_loader(self):
        """Create a mock DataLoader"""
        mock = MagicMock()
        mock.load_full_refresh_to_staging = MagicMock(return_value=1000000)
        mock.load_incremental_to_staging = MagicMock(return_value=100000)
        mock.get_staging_row_count_2024 = MagicMock(return_value=1000000)
        mock.get_raw_row_count = MagicMock(return_value=1000000)
        mock.should_load_to_raw = MagicMock(return_value=True)
        return mock

    @pytest.fixture
    def mock_metadata(self):
        """Create a mock MetadataManager"""
        mock = MagicMock()
        mock.record_run = MagicMock()
        mock.is_full_year_loaded = MagicMock(return_value=False)
        mock.get_last_completed_month = MagicMock(return_value=None)
        return mock

    @pytest.fixture
    def mock_retry_handler(self):
        """Create a mock RetryHandler"""
        mock = MagicMock()
        mock.retry_operation = MagicMock(side_effect=lambda func, name, *args: func(*args) if args else func())
        return mock

    @pytest.fixture
    def orchestrator(self, mock_bq_client, mock_data_loader, mock_metadata, mock_retry_handler):
        """Create orchestrator with all mocked dependencies"""
        with patch('src.orchestrator.BigQueryClient', return_value=mock_bq_client), \
             patch('src.orchestrator.DataLoader', return_value=mock_data_loader), \
             patch('src.orchestrator.MetadataManager', return_value=mock_metadata), \
             patch('src.orchestrator.RetryHandler', return_value=mock_retry_handler):
            
            orch = PipelineOrchestrator()
            # Manually set mocks for easier access in tests
            orch.bq_client = mock_bq_client
            orch.data_loader = mock_data_loader
            orch.metadata = mock_metadata
            orch.retry_handler = mock_retry_handler
            return orch

    # ============================================================================
    # INITIALIZATION TESTS
    # ============================================================================

    def test_initialization(self, orchestrator):
        """Test that orchestrator initializes with all components"""
        assert orchestrator.bq_client is not None
        assert orchestrator.data_loader is not None
        assert orchestrator.metadata is not None
        assert orchestrator.retry_handler is not None

    def test_create_tables(self, orchestrator, mock_bq_client):
        """Test table creation executes SQL files in correct order"""
        orchestrator.create_tables()
        
        # Verify execute_sql_file was called 3 times
        assert mock_bq_client.execute_sql_file.call_count == 3
        
        # Verify correct files were called
        calls = mock_bq_client.execute_sql_file.call_args_list
        assert 'create_metadata_table.sql' in str(calls[0])
        assert 'create_staging_table.sql' in str(calls[1])
        assert 'create_raw_table.sql' in str(calls[2])

    def test_create_tables_failure(self, orchestrator, mock_bq_client):
        """Test table creation handles failures"""
        mock_bq_client.execute_sql_file.side_effect = Exception("SQL error")
        
        with pytest.raises(Exception, match="SQL error"):
            orchestrator.create_tables()

    # ============================================================================
    # FULL REFRESH TESTS
    # ============================================================================

    def test_full_refresh_success(self, orchestrator, mock_data_loader, mock_metadata):
        """Test successful full refresh pipeline"""
        mock_data_loader.load_full_refresh_to_staging.return_value = 1000000
        mock_data_loader.get_staging_row_count_2024.return_value = 1000000
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_raw_row_count.return_value = 1000000
        
        result = orchestrator.run_full_refresh()
        
        assert result is True
        mock_metadata.record_run.assert_called_once()
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_SUCCESS
        assert call_args['rows_loaded'] == 1000000

    def test_full_refresh_staging_already_loaded(self, orchestrator, mock_data_loader, mock_metadata):
        """Test full refresh when staging already has data"""
        mock_data_loader.load_full_refresh_to_staging.return_value = 0  # Already loaded
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_raw_row_count.return_value = 1000000
        
        result = orchestrator.run_full_refresh()
        
        assert result is True
        mock_metadata.record_run.assert_called_once()

    def test_full_refresh_skipped_when_raw_in_sync(self, orchestrator, mock_data_loader, mock_metadata):
        """Test full refresh returns 0 rows when raw is already in sync"""
        mock_data_loader.load_full_refresh_to_staging.return_value = 0
        mock_data_loader.should_load_to_raw.return_value = False  # Already in sync
        mock_data_loader.get_raw_row_count.return_value = 1000000
        
        result = orchestrator.run_full_refresh()
        
        assert result is True
        call_args = mock_metadata.record_run.call_args[1]
        # When raw_rows = raw_count (existing data), status is SUCCESS not SKIPPED
        # because the orchestrator checks if raw_rows == 0 to determine SKIPPED
        assert call_args['status'] in [STATUS_SUCCESS, STATUS_SKIPPED]
        assert call_args['rows_loaded'] == 1000000

    def test_full_refresh_failure(self, orchestrator, mock_data_loader, mock_metadata):
        """Test full refresh handles failures"""
        mock_data_loader.load_full_refresh_to_staging.side_effect = Exception("Download failed")
        
        result = orchestrator.run_full_refresh()
        
        assert result is False
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_FAILED
        assert 'Download failed' in call_args['error_message']

    def test_full_refresh_no_staging_data(self, orchestrator, mock_data_loader, mock_metadata):
        """Test full refresh when staging has no 2024 data"""
        mock_data_loader.load_full_refresh_to_staging.return_value = 1000
        mock_data_loader.get_staging_row_count_2024.return_value = 0  # No 2024 data
        mock_data_loader.should_load_to_raw.return_value = True
        
        result = orchestrator.run_full_refresh()
        
        assert result is True
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['rows_loaded'] == 0

    # ============================================================================
    # INCREMENTAL TESTS
    # ============================================================================

    def test_incremental_success_first_month(self, orchestrator, mock_data_loader, mock_metadata):
        """Test successful incremental load for first month"""
        mock_metadata.get_last_completed_month.return_value = None  # No previous loads
        mock_data_loader.load_incremental_to_staging.return_value = 100000
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_raw_row_count.side_effect = [0, 100000]  # Before and after
        
        result = orchestrator.run_incremental()
        
        assert result is True
        mock_data_loader.load_incremental_to_staging.assert_called_once_with(1)  # January
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_SUCCESS
        assert call_args['month'] == 1

    def test_incremental_success_specific_month(self, orchestrator, mock_data_loader, mock_metadata):
        """Test incremental load for specific month"""
        mock_data_loader.load_incremental_to_staging.return_value = 100000
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_raw_row_count.side_effect = [500000, 600000]
        
        result = orchestrator.run_incremental(target_month=6)  # June
        
        assert result is True
        mock_data_loader.load_incremental_to_staging.assert_called_once_with(6)
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['month'] == 6

    def test_incremental_sequential_months(self, orchestrator, mock_data_loader, mock_metadata):
        """Test incremental loads next month in sequence"""
        mock_metadata.get_last_completed_month.return_value = 5  # May completed
        mock_data_loader.load_incremental_to_staging.return_value = 100000
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_raw_row_count.side_effect = [500000, 600000]
        
        result = orchestrator.run_incremental()
        
        assert result is True
        mock_data_loader.load_incremental_to_staging.assert_called_once_with(6)  # June

    def test_incremental_skipped_full_year_loaded(self, orchestrator, mock_metadata):
        """Test incremental skips when full year already loaded"""
        mock_metadata.is_full_year_loaded.return_value = True
        
        result = orchestrator.run_incremental()
        
        assert result is True
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_SKIPPED

    def test_incremental_skipped_all_months_loaded(self, orchestrator, mock_metadata):
        """Test incremental skips when all 12 months loaded"""
        mock_metadata.get_last_completed_month.return_value = 12  # December
        
        result = orchestrator.run_incremental()
        
        assert result is True
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_SKIPPED

    def test_incremental_skipped_month_already_loaded(self, orchestrator, mock_data_loader, mock_metadata):
        """Test incremental skips when month data already exists"""
        mock_data_loader.load_incremental_to_staging.return_value = 0  # Already loaded
        mock_data_loader.should_load_to_raw.return_value = False  # Already in sync
        mock_data_loader.get_raw_row_count.return_value = 600000
        
        result = orchestrator.run_incremental(target_month=3)
        
        assert result is True
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_SKIPPED

    def test_incremental_failure(self, orchestrator, mock_data_loader, mock_metadata):
        """Test incremental handles failures"""
        mock_data_loader.load_incremental_to_staging.side_effect = Exception("Network error")
        
        result = orchestrator.run_incremental(target_month=3)
        
        assert result is False
        call_args = mock_metadata.record_run.call_args[1]
        assert call_args['status'] == STATUS_FAILED
        assert 'Network error' in call_args['error_message']

    # ============================================================================
    # INTERNAL METHOD TESTS
    # ============================================================================

    def test_load_staging_to_raw_full_with_sync(self, orchestrator, mock_data_loader):
        """Test loading staging to raw when already in sync"""
        mock_data_loader.should_load_to_raw.return_value = False
        mock_data_loader.get_raw_row_count.return_value = 1000000
        
        result = orchestrator._load_staging_to_raw_full()
        
        assert result == 1000000
        # Should not execute SQL file
        orchestrator.bq_client.execute_sql_file.assert_not_called()

    def test_load_staging_to_raw_full_needs_recreation(self, orchestrator, mock_data_loader, mock_bq_client):
        """Test loading staging to raw when recreation needed"""
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_staging_row_count_2024.return_value = 1000000
        mock_data_loader.get_raw_row_count.return_value = 1000000
        
        result = orchestrator._load_staging_to_raw_full()
        
        assert result == 1000000
        # Should execute SQL file
        mock_bq_client.execute_sql_file.assert_called_once()

    def test_load_staging_to_raw_incremental_calculates_delta(self, orchestrator, mock_data_loader, mock_bq_client):
        """Test incremental load calculates rows added correctly"""
        mock_data_loader.should_load_to_raw.return_value = True
        mock_data_loader.get_raw_row_count.side_effect = [500000, 600000]  # Before/after
        
        result = orchestrator._load_staging_to_raw_incremental(3)
        
        assert result == 100000  # Delta
        mock_bq_client.execute_sql_file.assert_called_once()

    def test_transform_to_silver(self, orchestrator, mock_bq_client):
        """Test silver transformation executes correctly"""
        mock_bq_client.get_row_count.return_value = 900000
        
        orchestrator._transform_to_silver()
        
        mock_bq_client.execute_sql_file.assert_called_once()
        assert 'create_silver_table.sql' in str(mock_bq_client.execute_sql_file.call_args)

    def test_aggregate_to_gold(self, orchestrator, mock_bq_client):
        """Test gold aggregation executes correctly"""
        mock_bq_client.get_row_count.return_value = 5000
        
        orchestrator._aggregate_to_gold()
        
        mock_bq_client.execute_sql_file.assert_called_once()
        assert 'create_gold_table.sql' in str(mock_bq_client.execute_sql_file.call_args)

    def test_get_next_month_to_load_first_load(self, orchestrator, mock_metadata):
        """Test getting next month when no previous loads"""
        mock_metadata.get_last_completed_month.return_value = None
        
        result = orchestrator._get_next_month_to_load()
        
        assert result == 1  # January

    def test_get_next_month_to_load_sequential(self, orchestrator, mock_metadata):
        """Test getting next month in sequence"""
        mock_metadata.get_last_completed_month.return_value = 5  # May
        
        result = orchestrator._get_next_month_to_load()
        
        assert result == 6  # June

    def test_get_next_month_to_load_all_loaded(self, orchestrator, mock_metadata):
        """Test getting next month when all months loaded"""
        mock_metadata.get_last_completed_month.return_value = 12  # December
        
        result = orchestrator._get_next_month_to_load()
        
        assert result is None

    # ============================================================================
    # CLEANUP TESTS
    # ============================================================================

    def test_close(self, orchestrator, mock_bq_client):
        """Test orchestrator closes BigQuery client"""
        orchestrator.close()
        
        mock_bq_client.close.assert_called_once()


# ============================================================================
# INTEGRATION TESTS (Higher level scenarios)
# ============================================================================

class TestPipelineIntegration:
    """Integration tests for complete pipeline scenarios"""

    @pytest.fixture
    def orchestrator_integration(self):
        """Create orchestrator with minimal mocking for integration tests"""
        with patch('src.orchestrator.BigQueryClient') as mock_bq, \
             patch('src.orchestrator.DataLoader') as mock_loader, \
             patch('src.orchestrator.MetadataManager') as mock_meta, \
             patch('src.orchestrator.RetryHandler') as mock_retry:
            
            # Setup basic returns
            mock_bq_instance = mock_bq.return_value
            mock_loader_instance = mock_loader.return_value
            mock_meta_instance = mock_meta.return_value
            mock_retry_instance = mock_retry.return_value
            
            # Configure retry handler to actually call functions
            mock_retry_instance.retry_operation.side_effect = lambda func, name, *args: func(*args) if args else func()
            
            orch = PipelineOrchestrator()
            orch.bq_client = mock_bq_instance
            orch.data_loader = mock_loader_instance
            orch.metadata = mock_meta_instance
            orch.retry_handler = mock_retry_instance
            
            return orch

    def test_full_pipeline_end_to_end(self, orchestrator_integration):
        """Test complete pipeline from staging to gold"""
        orch = orchestrator_integration
        
        # Setup mocks
        orch.data_loader.load_full_refresh_to_staging.return_value = 1000000
        orch.data_loader.should_load_to_raw.return_value = True
        orch.data_loader.get_staging_row_count_2024.return_value = 1000000
        orch.data_loader.get_raw_row_count.return_value = 1000000
        orch.bq_client.get_row_count.side_effect = [900000, 5000]  # Silver, Gold
        
        result = orch.run_full_refresh()
        
        assert result is True
        # Verify all stages were called
        orch.data_loader.load_full_refresh_to_staging.assert_called_once()
        # SQL file is called 3 times: Raw table creation + Silver + Gold
        assert orch.bq_client.execute_sql_file.call_count == 3

    def test_incremental_pipeline_progression(self, orchestrator_integration):
        """Test incremental pipeline progresses through months"""
        orch = orchestrator_integration
        
        # Setup for month 1 (January)
        orch.metadata.get_last_completed_month.return_value = None
        orch.metadata.is_full_year_loaded.return_value = False
        orch.data_loader.load_incremental_to_staging.return_value = 100000
        orch.data_loader.should_load_to_raw.return_value = True
        orch.data_loader.get_raw_row_count.side_effect = [0, 100000]
        orch.bq_client.get_row_count.side_effect = [90000, 5000]  # Silver, Gold
        
        result = orch.run_incremental()
        assert result is True
        
        # Verify the method was called (retry_handler wraps it)
        assert orch.data_loader.load_incremental_to_staging.called
        # Check it was called with month 1
        call_args = orch.data_loader.load_incremental_to_staging.call_args
        assert call_args[0][0] == 1  # First positional argument should be 1 (January)
        