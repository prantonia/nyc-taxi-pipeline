"""
Tests for pipeline orchestrator.
"""
import pytest
from unittest.mock import Mock, MagicMock, patch

from src.config import STATUS_SUCCESS, STATUS_FAILED, STATUS_SKIPPED


class TestPipelineOrchestrator:
    """Test cases for PipelineOrchestrator class."""
    
    @pytest.fixture
    def mock_dependencies(self):
        """Create mocks for all orchestrator dependencies."""
        with patch('src.orchestrator.BigQueryClient') as mock_bq, \
             patch('src.orchestrator.DataLoader') as mock_loader, \
             patch('src.orchestrator.MetadataManager') as mock_metadata, \
             patch('src.orchestrator.RetryHandler') as mock_retry:
            
            # Setup mock instances
            bq_instance = Mock()
            loader_instance = Mock()
            metadata_instance = Mock()
            retry_instance = Mock()
            
            mock_bq.return_value = bq_instance
            mock_loader.return_value = loader_instance
            mock_metadata.return_value = metadata_instance
            mock_retry.return_value = retry_instance
            
            # Configure retry handler to just call the function
            def retry_side_effect(func, name, *args, **kwargs):
                return func(*args, **kwargs)
            
            retry_instance.retry_operation.side_effect = retry_side_effect
            
            yield {
                'bq_client': bq_instance,
                'data_loader': loader_instance,
                'metadata': metadata_instance,
                'retry_handler': retry_instance
            }
    
    def test_initialization(self, mock_dependencies):
        """Test orchestrator initialization."""
        from src.orchestrator import PipelineOrchestrator
        
        orchestrator = PipelineOrchestrator()
        
        assert orchestrator.bq_client is not None
        assert orchestrator.data_loader is not None
        assert orchestrator.metadata is not None
        assert orchestrator.retry_handler is not None
    
    def test_full_refresh_success(self, mock_dependencies):
        """Test successful full refresh pipeline."""
        from src.orchestrator import PipelineOrchestrator
        
        # Configure mocks
        mock_dependencies['data_loader'].load_full_refresh_to_staging.return_value = 1000000
        mock_dependencies['data_loader'].get_staging_row_count_2024.return_value = 1000000
        mock_dependencies['data_loader'].get_raw_row_count.return_value = 0
        mock_dependencies['data_loader'].load_staging_to_raw_full.return_value = 1000000
        mock_dependencies['bq_client'].get_row_count.return_value = 1000000
        
        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_full_refresh()
        
        assert result is True
        assert mock_dependencies['metadata'].record_run.called
    
    def test_full_refresh_skipped(self, mock_dependencies):
        """Test full refresh pipeline when data already loaded."""
        from src.orchestrator import PipelineOrchestrator
        
        # Configure mocks - staging not empty, row counts match
        mock_dependencies['data_loader'].load_full_refresh_to_staging.return_value = 0
        mock_dependencies['data_loader'].get_staging_row_count_2024.return_value = 1000000
        mock_dependencies['data_loader'].get_raw_row_count.return_value = 1000000
        mock_dependencies['bq_client'].get_row_count.return_value = 1000000
        
        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_full_refresh()
        
        assert result is True
        # Verify metadata recorded with SKIPPED status
        call_args = mock_dependencies['metadata'].record_run.call_args
        assert call_args[1]['status'] == STATUS_SKIPPED
        assert call_args[1]['rows_loaded'] == 0
    
    def test_incremental_success(self, mock_dependencies):
        """Test successful incremental pipeline."""
        from src.orchestrator import PipelineOrchestrator
        
        # Configure mocks
        mock_dependencies['metadata'].is_full_year_loaded.return_value = False
        mock_dependencies['metadata'].get_last_successful_month.return_value = None  # Start with January
        mock_dependencies['data_loader'].load_incremental_to_staging.return_value = 100000
        mock_dependencies['data_loader'].month_exists_in_raw.return_value = False
        mock_dependencies['data_loader'].load_staging_to_raw_incremental.return_value = 100000
        mock_dependencies['bq_client'].get_row_count.return_value = 100000
        
        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_incremental()
        
        assert result is True
        assert mock_dependencies['metadata'].record_run.called
    
    def test_incremental_month_already_loaded(self, mock_dependencies):
        """Test incremental pipeline when month already loaded."""
        from src.orchestrator import PipelineOrchestrator
        
        # Configure mocks
        mock_dependencies['metadata'].is_full_year_loaded.return_value = False
        mock_dependencies['metadata'].get_last_successful_month.return_value = None
        mock_dependencies['data_loader'].load_incremental_to_staging.return_value = 0
        mock_dependencies['data_loader'].month_exists_in_raw.return_value = True
        mock_dependencies['bq_client'].get_row_count.return_value = 100000
        
        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_incremental()
        
        assert result is True
        call_args = mock_dependencies['metadata'].record_run.call_args
        assert call_args[1]['status'] == STATUS_SKIPPED
    
    def test_incremental_full_year_loaded(self, mock_dependencies):
        """Test incremental pipeline when full year already loaded."""
        from src.orchestrator import PipelineOrchestrator
        
        # Configure mocks
        mock_dependencies['metadata'].is_full_year_loaded.return_value = True
        
        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_incremental()
        
        assert result is True
        call_args = mock_dependencies['metadata'].record_run.call_args
        assert call_args[1]['status'] == STATUS_SKIPPED
    
    def test_get_next_month_first_load(self, mock_dependencies):
        """Test getting next month when no months loaded."""
        from src.orchestrator import PipelineOrchestrator
        
        mock_dependencies['metadata'].get_last_successful_month.return_value = None
        
        orchestrator = PipelineOrchestrator()
        next_month = orchestrator._get_next_month_to_load()
        
        assert next_month == 1  # Should start with January
    
    def test_get_next_month_sequential(self, mock_dependencies):
        """Test getting next month in sequence."""
        from src.orchestrator import PipelineOrchestrator
        
        mock_dependencies['metadata'].get_last_successful_month.return_value = 5  # May loaded
        
        orchestrator = PipelineOrchestrator()
        next_month = orchestrator._get_next_month_to_load()
        
        assert next_month == 6  # Should load June next
    
    def test_get_next_month_all_loaded(self, mock_dependencies):
        """Test getting next month when all 12 months loaded."""
        from src.orchestrator import PipelineOrchestrator
        
        mock_dependencies['metadata'].get_last_successful_month.return_value = 12
        
        orchestrator = PipelineOrchestrator()
        next_month = orchestrator._get_next_month_to_load()
        
        assert next_month is None  # All months loaded
    
    def test_pipeline_failure_recorded(self, mock_dependencies):
        """Test that pipeline failures are properly recorded."""
        from src.orchestrator import PipelineOrchestrator
        
        # Make data loader raise an exception
        mock_dependencies['data_loader'].load_full_refresh_to_staging.side_effect = Exception("Test error")
        mock_dependencies['retry_handler'].retry_operation.side_effect = Exception("Test error")
        
        orchestrator = PipelineOrchestrator()
        result = orchestrator.run_full_refresh()
        
        assert result is False
        call_args = mock_dependencies['metadata'].record_run.call_args
        assert call_args[1]['status'] == STATUS_FAILED
        assert 'Test error' in call_args[1]['error_message']


class TestIdempotencyLogic:
    """Test cases for idempotency checks."""
    
    @pytest.fixture
    def mock_dependencies(self):
        """Create mocks for testing idempotency."""
        with patch('src.orchestrator.BigQueryClient') as mock_bq, \
             patch('src.orchestrator.DataLoader') as mock_loader, \
             patch('src.orchestrator.MetadataManager') as mock_metadata, \
             patch('src.orchestrator.RetryHandler') as mock_retry:
            
            bq_instance = Mock()
            loader_instance = Mock()
            metadata_instance = Mock()
            retry_instance = Mock()
            
            mock_bq.return_value = bq_instance
            mock_loader.return_value = loader_instance
            mock_metadata.return_value = metadata_instance
            mock_retry.return_value = retry_instance
            
            def retry_side_effect(func, name, *args, **kwargs):
                return func(*args, **kwargs)
            
            retry_instance.retry_operation.side_effect = retry_side_effect
            
            yield {
                'bq_client': bq_instance,
                'data_loader': loader_instance,
                'metadata': metadata_instance,
                'retry_handler': retry_instance
            }
    
    def test_staging_skip_if_not_empty(self, mock_dependencies):
        """Test that staging download is skipped if data exists."""
        from src.orchestrator import PipelineOrchestrator
        
        mock_dependencies['data_loader'].load_full_refresh_to_staging.return_value = 0
        mock_dependencies['data_loader'].get_staging_row_count_2024.return_value = 1000000
        mock_dependencies['data_loader'].get_raw_row_count.return_value = 0
        mock_dependencies['data_loader'].load_staging_to_raw_full.return_value = 1000000
        mock_dependencies['bq_client'].get_row_count.return_value = 100000
        
        orchestrator = PipelineOrchestrator()
        orchestrator.run_full_refresh()
        
        # Verify download was skipped (returned 0)
        assert mock_dependencies['data_loader'].load_full_refresh_to_staging.called
    
    def test_raw_skip_if_counts_match(self, mock_dependencies):
        """Test that raw load is skipped if row counts match."""
        from src.orchestrator import PipelineOrchestrator
        
        mock_dependencies['data_loader'].load_full_refresh_to_staging.return_value = 0
        mock_dependencies['data_loader'].get_staging_row_count_2024.return_value = 1000000
        mock_dependencies['data_loader'].get_raw_row_count.return_value = 1000000  # Same count
        mock_dependencies['bq_client'].get_row_count.return_value = 100000
        
        orchestrator = PipelineOrchestrator()
        orchestrator.run_full_refresh()
        
        # Verify load to raw was skipped
        assert not mock_dependencies['data_loader'].load_staging_to_raw_full.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])