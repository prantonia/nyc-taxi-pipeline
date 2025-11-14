"""
Main orchestrator for NYC Taxi Data Pipeline.
Coordinates all pipeline operations across layers with idempotency checks.
"""
import logging
import time
from datetime import datetime
from typing import Optional

from src.bigquery_client import BigQueryClient
from src.data_loader import DataLoader
from src.metadata_manager import MetadataManager
from src.retry_handler import RetryHandler
from src.config import (
    PIPELINE_FULL_REFRESH,
    PIPELINE_INCREMENTAL,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_SKIPPED,
    SQL_DIR,
    SILVER_TABLE,
    GOLD_TABLE
)

# Setup logger
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """Main orchestrator for the data pipeline."""
    
    def __init__(self):
        """Initialize orchestrator with all required components."""
        logger.info("Initializing Pipeline Orchestrator")
        
        self.bq_client = BigQueryClient()
        self.data_loader = DataLoader(self.bq_client)
        self.metadata = MetadataManager(self.bq_client)
        self.retry_handler = RetryHandler()
        
        logger.info("Pipeline Orchestrator initialized")
    
    def create_tables(self):
        """Create all required BigQuery tables."""
        try:
            logger.info("Creating BigQuery tables")
            
            import os
            
            # Create tables in order
            tables = [
                'create_metadata_table.sql',
                'create_staging_table.sql',
                'create_raw_table.sql'
            ]
            
            for table_file in tables:
                filepath = os.path.join(SQL_DIR, table_file)
                logger.info(f"Executing {table_file}")
                self.bq_client.execute_sql_file(filepath)
            
            logger.info("All tables created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise
    
    def run_full_refresh(self):
        """
        Execute full refresh pipeline.
        Loads all 2024 data at once with idempotency checks.
        """
        start_time = time.time()
        pipeline_name = PIPELINE_FULL_REFRESH
        
        logger.info("="*50)
        logger.info("STARTING FULL REFRESH PIPELINE")
        logger.info("="*50)
        
        try:
            # Step 1: Load to Staging (only if empty)
            logger.info("\n[STEP 1/4] Loading data to Staging layer")
            staging_rows = self.retry_handler.retry_operation(
                self.data_loader.load_full_refresh_to_staging,
                "Load to Staging"
            )
            
            if staging_rows == 0:
                logger.info("Staging data already exists, skipping download")
            else:
                logger.info(f"Loaded {staging_rows:,} rows to staging")
            
            # Step 2: Load to Raw (with idempotency check)
            logger.info("\n[STEP 2/4] Loading data to Raw layer")
            raw_rows = self._load_staging_to_raw_full()
            
            # Step 3: Transform to Silver
            logger.info("\n[STEP 3/4] Transforming data to Silver layer")
            self.retry_handler.retry_operation(
                self._transform_to_silver,
                "Transform to Silver"
            )
            
            # Step 4: Aggregate to Gold
            logger.info("\n[STEP 4/4] Aggregating data to Gold layer")
            self.retry_handler.retry_operation(
                self._aggregate_to_gold,
                "Aggregate to Gold"
            )
            
            # Record success
            runtime = time.time() - start_time
            
            if raw_rows == 0:
                # Data was skipped
                self.metadata.record_run(
                    pipeline_name=pipeline_name,
                    status=STATUS_SKIPPED,
                    rows_loaded=0,
                    month=None,
                    runtime=runtime
                )
                logger.info("\n" + "="*80)
                logger.info("FULL REFRESH PIPELINE COMPLETED - SKIPPED (data already loaded)")
                logger.info(f"Runtime: {runtime:.2f} seconds")
                logger.info("="*80)
            else:
                self.metadata.record_run(
                    pipeline_name=pipeline_name,
                    status=STATUS_SUCCESS,
                    rows_loaded=raw_rows,
                    month=None,
                    runtime=runtime
                )
                logger.info("\n" + "="*80)
                logger.info("FULL REFRESH PIPELINE COMPLETED SUCCESSFULLY")
                logger.info(f"Rows Loaded: {raw_rows:,}")
                logger.info(f"Runtime: {runtime:.2f} seconds")
                logger.info("="*80)
            
            return True
            
        except Exception as e:
            runtime = time.time() - start_time
            error_msg = str(e)
            
            logger.error(f"\nFull refresh pipeline failed: {error_msg}")
            
            self.metadata.record_run(
                pipeline_name=pipeline_name,
                status=STATUS_FAILED,
                rows_loaded=0,
                month=None,
                runtime=runtime,
                error_message=error_msg
            )
            
            logger.info("\n" + "="*50)
            logger.info("FULL REFRESH PIPELINE FAILED")
            logger.info(f"Error: {error_msg}")
            logger.info(f"Runtime: {runtime:.2f} seconds")
            logger.info("="*50)
            
            return False
    
    def run_incremental(self, target_month: Optional[int] = None):
        """
        Execute incremental pipeline.
        Loads data month by month with automatic progression.
        
        Args:
            target_month: Specific month to load (1-12), None for next month
        """
        start_time = time.time()
        pipeline_name = PIPELINE_INCREMENTAL
        
        logger.info("="*50)
        logger.info("STARTING INCREMENTAL PIPELINE")
        logger.info("="*50)
        
        try:
            # Check if full year already loaded
            if self.metadata.is_full_year_loaded():
                runtime = time.time() - start_time
                logger.info("Full year already loaded, skipping")
                
                self.metadata.record_run(
                    pipeline_name=pipeline_name,
                    status=STATUS_SKIPPED,
                    rows_loaded=0,
                    month=None,
                    runtime=runtime
                )
                
                logger.info("\n" + "="*50)
                logger.info("INCREMENTAL PIPELINE COMPLETED - SKIPPED (full year loaded)")
                logger.info(f"Runtime: {runtime:.2f} seconds")
                logger.info("="*50)
                
                return True
            
            # Determine which month to load
            if target_month is None:
                month_to_load = self._get_next_month_to_load()
            else:
                month_to_load = target_month
            
            if month_to_load is None:
                runtime = time.time() - start_time
                logger.info("All months already loaded")
                
                self.metadata.record_run(
                    pipeline_name=pipeline_name,
                    status=STATUS_SKIPPED,
                    rows_loaded=0,
                    month=None,
                    runtime=runtime
                )
                
                logger.info("\n" + "="*50)
                logger.info("INCREMENTAL PIPELINE COMPLETED - SKIPPED (all months loaded)")
                logger.info(f"Runtime: {runtime:.2f} seconds")
                logger.info("="*50)
                
                return True
            
            from src.config import get_month_name
            month_name = get_month_name(month_to_load)
            logger.info(f"\nProcessing: {month_name} 2024")
            
            # Step 1: Load month to Staging
            logger.info(f"\n[STEP 1/4] Loading {month_name} to Staging layer")
            staging_rows = self.retry_handler.retry_operation(
                self.data_loader.load_incremental_to_staging,
                f"Load {month_name} to Staging",
                month_to_load
            )
            
            if staging_rows == 0:
                logger.info(f"{month_name} data already exists in staging")
            else:
                logger.info(f"Loaded {staging_rows:,} rows to staging")
            
            # Step 2: Load month to Raw (with idempotency check)
            logger.info(f"\n[STEP 2/4] Loading {month_name} to Raw layer")
            raw_rows = self._load_staging_to_raw_incremental(month_to_load)
            
            # Step 3: Transform to Silver
            logger.info(f"\n[STEP 3/4] Transforming data to Silver layer")
            self.retry_handler.retry_operation(
                self._transform_to_silver,
                "Transform to Silver"
            )
            
            # Step 4: Aggregate to Gold
            logger.info(f"\n[STEP 4/4] Aggregating data to Gold layer")
            self.retry_handler.retry_operation(
                self._aggregate_to_gold,
                "Aggregate to Gold"
            )
            
            # Record success
            runtime = time.time() - start_time
            
            if raw_rows == 0:
                self.metadata.record_run(
                    pipeline_name=pipeline_name,
                    status=STATUS_SKIPPED,
                    rows_loaded=0,
                    month=month_to_load,
                    runtime=runtime
                )
                logger.info("\n" + "="*50)
                logger.info(f"INCREMENTAL PIPELINE COMPLETED - SKIPPED ({month_name} already loaded)")
                logger.info(f"Runtime: {runtime:.2f} seconds")
                logger.info("="*50)
            else:
                self.metadata.record_run(
                    pipeline_name=pipeline_name,
                    status=STATUS_SUCCESS,
                    rows_loaded=raw_rows,
                    month=month_to_load,
                    runtime=runtime
                )
                logger.info("\n" + "="*50)
                logger.info("INCREMENTAL PIPELINE COMPLETED SUCCESSFULLY")
                logger.info(f"Month Loaded: {month_name}")
                logger.info(f"Rows Loaded: {raw_rows:,}")
                logger.info(f"Runtime: {runtime:.2f} seconds")
                logger.info("="*50)
            
            return True
            
        except Exception as e:
            runtime = time.time() - start_time
            error_msg = str(e)
            
            logger.error(f"\n✗ Incremental pipeline failed: {error_msg}")
            
            self.metadata.record_run(
                pipeline_name=pipeline_name,
                status=STATUS_FAILED,
                rows_loaded=0,
                month=month_to_load if target_month or 'month_to_load' in locals() else None,
                runtime=runtime,
                error_message=error_msg
            )
            
            logger.info("\n" + "="*50)
            logger.info("INCREMENTAL PIPELINE FAILED")
            logger.info(f"Error: {error_msg}")
            logger.info(f"Runtime: {runtime:.2f} seconds")
            logger.info("="*50)
            
            return False
    
    def _load_staging_to_raw_full(self) -> int:
        """
        Load full year from staging to raw by recreating table.
        Uses idempotency check to skip if raw is already in sync.
        """
        logger.info("Checking if raw table needs recreation...")
        
        # Idempotency check
        if not self.data_loader.should_load_to_raw():
            logger.info("Raw table already in sync, skipping recreation")
            raw_count = self.data_loader.get_raw_row_count()
            return raw_count
        
        logger.info("Loading 2024 data from staging to raw (CREATE OR REPLACE)")
        
        # Check if staging has 2024 data
        staging_count = self.data_loader.get_staging_row_count_2024()
        
        logger.info(f"Staging 2024 rows: {staging_count:,}")
        
        if staging_count == 0:
            logger.warning("No 2024 data found in staging!")
            return 0
        
        # Recreate raw table from staging (filters 2024 data automatically)
        import os
        filepath = os.path.join(SQL_DIR, 'create_raw_table.sql')
        
        logger.info("Executing CREATE OR REPLACE for raw table")
        self.bq_client.execute_sql_file(filepath)
        
        # Get count from new raw table
        raw_count = self.data_loader.get_raw_row_count()
        logger.info(f"Raw table recreated with {raw_count:,} rows")
        
        return raw_count
    
    def _load_staging_to_raw_incremental(self, month: int) -> int:
        """
        Load specific month from staging to raw by recreating entire raw table.
        Uses idempotency check to skip if raw is already in sync.
        
        Returns:
            Number of rows loaded for THIS MONTH ONLY (not cumulative)
        """
        from src.config import get_month_name
        month_name = get_month_name(month)
        
        logger.info(f"Checking if raw table needs recreation after loading {month_name}...")
        
        # Get count BEFORE recreation for comparison
        raw_count_before = self.data_loader.get_raw_row_count()
        
        # Idempotency check
        if not self.data_loader.should_load_to_raw():
            logger.info("Raw table already in sync, skipping recreation")
            # Return 0 because no new rows were added in this run
            return 0
        
        logger.info(f"Loading {month_name} to raw (CREATE OR REPLACE entire table)")
        
        # Recreate raw table from staging (includes all 2024 data loaded so far)
        import os
        filepath = os.path.join(SQL_DIR, 'create_raw_table.sql')
        
        logger.info("Executing CREATE OR REPLACE for raw table")
        self.bq_client.execute_sql_file(filepath)
        
        # Get count AFTER recreation
        raw_count_after = self.data_loader.get_raw_row_count()
        logger.info(f"Raw table recreated with {raw_count_after:,} total rows")
        
        # Calculate rows added in THIS run only
        rows_added_this_month = raw_count_after - raw_count_before
        logger.info(f"Added {rows_added_this_month:,} new rows for {month_name}")
        
        return rows_added_this_month
    
    def _transform_to_silver(self):
        """Transform raw data to silver layer (drop and recreate)."""
        import os
        filepath = os.path.join(SQL_DIR, 'create_silver_table.sql')
        
        logger.info("Dropping and recreating silver table")
        self.bq_client.execute_sql_file(filepath)
        
        row_count = self.bq_client.get_row_count(SILVER_TABLE)
        logger.info(f"Silver table created with {row_count:,} rows")
    
    def _aggregate_to_gold(self):
        """Aggregate silver data to gold layer (drop and recreate)."""
        import os
        filepath = os.path.join(SQL_DIR, 'create_gold_table.sql')
        
        logger.info("Dropping and recreating gold table")
        self.bq_client.execute_sql_file(filepath)
        
        row_count = self.bq_client.get_row_count(GOLD_TABLE)
        logger.info(f"Gold table created with {row_count:,} rows")
    
    def _get_next_month_to_load(self) -> Optional[int]:
        """
        Determine the next month to load for incremental pipeline.
        Considers both SUCCESS and SKIPPED months as completed.
        """
        # Get last completed month (SUCCESS or SKIPPED)
        last_month = self.metadata.get_last_completed_month()
        
        if last_month is None:
            # No months loaded yet, start with January
            logger.info("No previous loads found, starting with January")
            return 1
        
        if last_month == 12:
            # All months loaded
            logger.info("All 12 months already loaded")
            return None
        
        next_month = last_month + 1
        from src.config import get_month_name
        logger.info(f"Last loaded: {get_month_name(last_month)}, Next: {get_month_name(next_month)}")
        
        return next_month
    
    def close(self):
        """Close all connections."""
        self.bq_client.close()
        logger.info("Pipeline orchestrator closed")


if __name__ == "__main__":
    # Test orchestrator
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    orchestrator = PipelineOrchestrator()
    
    print("\n✓ Orchestrator initialized successfully")
    print("\nTo run pipeline:")
    print("  Full Refresh: python run_full_refresh.py")
    print("  Incremental:  python run_incremental.py")
    
    orchestrator.close()