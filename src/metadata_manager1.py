"""
Metadata manager for tracking pipeline execution history.
Records all pipeline runs with detailed status and metrics.
"""
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List

from src.config import (
    METADATA_TABLE,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_SKIPPED,
    get_date_range_string,
    get_month_loaded_string
)

# Setup logger
logger = logging.getLogger(__name__)


class MetadataManager:
    """Manager for pipeline metadata tracking."""
    
    def __init__(self, bq_client):
        """
        Initialize metadata manager.
        
        Args:
            bq_client: BigQueryClient instance
        """
        self.bq_client = bq_client
        self.table_id = METADATA_TABLE
        logger.info("MetadataManager initialized")
    
    def record_run(
        self,
        pipeline_name: str,
        status: str,
        rows_loaded: int,
        month: Optional[int] = None,
        runtime: Optional[float] = None,
        error_message: Optional[str] = None
    ) -> None:
        """
        Record a pipeline run in the metadata table.
        
        Args:
            pipeline_name: Name of the pipeline (full_refresh or incremental)
            status: Run status (SUCCESS, FAILED, SKIPPED)
            rows_loaded: Number of rows loaded
            month: Month number for incremental loads (1-12), None for full refresh
            runtime: Execution time in seconds
            error_message: Error message if failed
        """
        try:
            date_range = get_date_range_string(month)
            month_loaded = get_month_loaded_string(month)
            run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Handle None values
            runtime_value = runtime if runtime is not None else 0
            # Escape single quotes in error message to prevent SQL injection
            error_msg = error_message.replace("'", "\\'") if error_message else ''
            
            query = f"""
                INSERT INTO `{self.table_id}` (
                    pipeline_name,
                    date_range,
                    month_loaded,
                    status,
                    rows_loaded,
                    run_timestamp,
                    runtime,
                    error_message
                )
                VALUES (
                    '{pipeline_name}',
                    '{date_range}',
                    '{month_loaded}',
                    '{status}',
                    {rows_loaded},
                    TIMESTAMP('{run_timestamp}'),
                    {runtime_value},
                    '{error_msg}'
                )
            """
            
            self.bq_client.execute_dml(query)
            logger.info(
                f"Metadata recorded: {pipeline_name} - {month_loaded} - "
                f"{status} - {rows_loaded} rows"
            )
            
        except Exception as e:
            logger.error(f"Failed to record metadata: {e}")
            # Don't raise - metadata failure shouldn't stop pipeline
    
    def get_last_successful_run(self, pipeline_name: str) -> Optional[Dict[str, Any]]:
        """
        Get details of the last successful run for a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Dictionary with run details or None if no successful runs
        """
        try:
            query = f"""
                SELECT 
                    pipeline_name,
                    date_range,
                    month_loaded,
                    status,
                    rows_loaded,
                    run_timestamp,
                    runtime,
                    error_message
                FROM `{self.table_id}`
                WHERE pipeline_name = '{pipeline_name}'
                    AND status = '{STATUS_SUCCESS}'
                ORDER BY run_timestamp DESC
                LIMIT 1
            """
            
            results = list(self.bq_client.execute_query(query))
            
            if results:
                row = results[0]
                return {
                    'pipeline_name': row.pipeline_name,
                    'date_range': row.date_range,
                    'month_loaded': row.month_loaded,
                    'status': row.status,
                    'rows_loaded': row.rows_loaded,
                    'run_timestamp': row.run_timestamp,
                    'runtime': row.runtime,
                    'error_message': row.error_message
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting last successful run: {e}")
            return None
    
    def get_last_attempted_month(self, pipeline_name: str = 'incremental') -> Optional[int]:
        """
        Get the last month attempted for incremental loads.
        
        Args:
            pipeline_name: Name of the pipeline (default: incremental)
            
        Returns:
            Month number (1-12) or None if no runs found
        """
        try:
            query = f"""
                SELECT month_loaded
                FROM `{self.table_id}`
                WHERE pipeline_name = '{pipeline_name}'
                    AND month_loaded != 'full year'
                ORDER BY run_timestamp DESC
                LIMIT 1
            """
            
            results = list(self.bq_client.execute_query(query))
            
            if results:
                month_name = results[0].month_loaded
                # Convert month name to number
                month_map = {
                    'January': 1, 'February': 2, 'March': 3, 'April': 4,
                    'May': 5, 'June': 6, 'July': 7, 'August': 8,
                    'September': 9, 'October': 10, 'November': 11, 'December': 12
                }
                return month_map.get(month_name)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting last attempted month: {e}")
            return None
    
    def get_last_successful_month(self, pipeline_name: str = 'incremental') -> Optional[int]:
        """
        Get the last successfully loaded month for incremental loads.
        
        Args:
            pipeline_name: Name of the pipeline (default: incremental)
            
        Returns:
            Month number (1-12) or None if no successful runs
        """
        try:
            query = f"""
                SELECT month_loaded
                FROM `{self.table_id}`
                WHERE pipeline_name = '{pipeline_name}'
                    AND status = '{STATUS_SUCCESS}'
                    AND month_loaded != 'full year'
                ORDER BY run_timestamp DESC
                LIMIT 1
            """
            
            results = list(self.bq_client.execute_query(query))
            
            if results:
                month_name = results[0].month_loaded
                # Convert month name to number
                month_map = {
                    'January': 1, 'February': 2, 'March': 3, 'April': 4,
                    'May': 5, 'June': 6, 'July': 7, 'August': 8,
                    'September': 9, 'October': 10, 'November': 11, 'December': 12
                }
                return month_map.get(month_name)
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting last successful month: {e}")
            return None
    
    def is_full_year_loaded(self) -> bool:
        """
        Check if the full year has been loaded successfully.
        
        Returns:
            True if full year is loaded, False otherwise
        """
        try:
            # Check if full_refresh completed successfully
            query = f"""
                SELECT COUNT(*) as count
                FROM `{self.table_id}`
                WHERE month_loaded = 'full year'
                    AND status = '{STATUS_SUCCESS}'
            """
            
            results = list(self.bq_client.execute_query(query))
            if results and results[0].count > 0:
                return True
            
            # Also check if all 12 months loaded via incremental
            query = f"""
                SELECT COUNT(DISTINCT month_loaded) as count
                FROM `{self.table_id}`
                WHERE pipeline_name = 'incremental'
                    AND status = '{STATUS_SUCCESS}'
                    AND month_loaded != 'full year'
            """
            
            results = list(self.bq_client.execute_query(query))
            if results and results[0].count == 12:
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking if full year loaded: {e}")
            return False
    
    def get_loaded_months(self) -> List[int]:
        """
        Get list of all successfully loaded months.
        
        Returns:
            List of month numbers that have been loaded
        """
        try:
            query = f"""
                SELECT DISTINCT month_loaded
                FROM `{self.table_id}`
                WHERE status = '{STATUS_SUCCESS}'
                    AND month_loaded != 'full year'
                ORDER BY month_loaded
            """
            
            results = list(self.bq_client.execute_query(query))
            
            month_map = {
                'January': 1, 'February': 2, 'March': 3, 'April': 4,
                'May': 5, 'June': 6, 'July': 7, 'August': 8,
                'September': 9, 'October': 10, 'November': 11, 'December': 12
            }
            
            months = [month_map[row.month_loaded] for row in results if row.month_loaded in month_map]
            return months
            
        except Exception as e:
            logger.error(f"Error getting loaded months: {e}")
            return []
    
    def get_run_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent run history.
        
        Args:
            limit: Number of recent runs to retrieve
            
        Returns:
            List of run records
        """
        try:
            query = f"""
                SELECT 
                    pipeline_name,
                    date_range,
                    month_loaded,
                    status,
                    rows_loaded,
                    run_timestamp,
                    runtime,
                    error_message
                FROM `{self.table_id}`
                ORDER BY run_timestamp DESC
                LIMIT {limit}
            """
            
            results = list(self.bq_client.execute_query(query))
            
            history = []
            for row in results:
                history.append({
                    'pipeline_name': row.pipeline_name,
                    'date_range': row.date_range,
                    'month_loaded': row.month_loaded,
                    'status': row.status,
                    'rows_loaded': row.rows_loaded,
                    'run_timestamp': row.run_timestamp,
                    'runtime': row.runtime,
                    'error_message': row.error_message
                })
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting run history: {e}")
            return []
    
    def log_run_history(self, limit: int = 10):
        """Log formatted run history."""
        history = self.get_run_history(limit)
        
        if not history:
            logger.info("No run history found")
            return
        
        logger.info("=" * 100)
        logger.info(f"Pipeline Run History (Last {limit} runs)")
        logger.info("=" * 100)
        
        for run in history:
            logger.info("")
            logger.info(f"Pipeline: {run['pipeline_name']}")
            logger.info(f"Month: {run['month_loaded']}")
            logger.info(f"Date Range: {run['date_range']}")
            logger.info(f"Status: {run['status']}")
            logger.info(f"Rows Loaded: {run['rows_loaded']:,}")
            logger.info(f"Runtime: {run['runtime']:.2f}s")
            logger.info(f"Timestamp: {run['run_timestamp']}")
            if run['error_message']:
                logger.info(f"Error: {run['error_message']}")
            logger.info("-" * 100)


if __name__ == "__main__":
    # Test metadata manager
    from src.bigquery_client import BigQueryClient
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        bq_client = BigQueryClient()
        metadata = MetadataManager(bq_client)
        
        logger.info("MetadataManager initialized")
        
        # Log run history
        metadata.log_run_history(limit=5)
        
    except Exception as e:
        logger.error(f"Error: {e}")