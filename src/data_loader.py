"""
Data loader module for downloading and loading NYC Taxi data.
Handles parquet file downloads and BigQuery data loading with PyArrow optimization.
"""
import logging
import requests
import pandas as pd
import pyarrow.parquet as pq
import io
from typing import Optional

from src.config import (
    TARGET_YEAR,
    STAGING_TABLE,
    RAW_TABLE,
    get_parquet_url,
    get_month_name,
)

# Setup logger
logger = logging.getLogger(__name__)


class DataLoader:
    """Handler for data loading operations with PyArrow optimization."""

    def __init__(self, bq_client):
        """
        Initialize data loader.

        Args:
            bq_client: BigQueryClient instance
        """
        self.bq_client = bq_client
        logger.info("DataLoader initialized")

    def check_if_data_exists_in_staging(self, df: pd.DataFrame) -> bool:
        """
        Check if downloaded data already exists in staging by comparing complete rows.
        Checks the row with minimum pickup_datetime and the row with maximum pickup_datetime.
        If both complete rows exist in staging, data is considered already loaded.

        Args:
            df: Downloaded DataFrame to check

        Returns:
            True if data already exists, False otherwise
        """
        try:
            if df is None or len(df) == 0:
                return False

            # Get the complete row with minimum pickup_datetime
            min_idx = df["tpep_pickup_datetime"].idxmin()
            min_row = df.loc[min_idx]

            # Get the complete row with maximum pickup_datetime
            max_idx = df["tpep_pickup_datetime"].idxmax()
            max_row = df.loc[max_idx]

            logger.debug(
                f"Checking for min row: pickup={min_row['tpep_pickup_datetime']}, dropoff={min_row['tpep_dropoff_datetime']}"
            )
            logger.debug(
                f"Checking for max row: pickup={max_row['tpep_pickup_datetime']}, dropoff={max_row['tpep_dropoff_datetime']}"
            )

            # Check if the row with min pickup_datetime exists in staging
            min_exists = self._check_row_exists_in_staging(min_row)

            # Check if the row with max pickup_datetime exists in staging
            max_exists = self._check_row_exists_in_staging(max_row)

            # Data exists only if BOTH boundary rows exist
            exists = min_exists and max_exists

            if exists:
                logger.info("Both boundary rows found in staging - data already exists")
            else:
                logger.info(
                    f"Boundary rows check: min_exists={min_exists}, max_exists={max_exists} - data needs upload"
                )

            return exists

        except Exception as e:
            logger.error(f"Error checking if data exists in staging: {e}")
            return False

    def _check_row_exists_in_staging(self, row: pd.Series) -> bool:
        """
        Check if a specific complete row exists in staging table.
        Matches on key columns: pickup_datetime, dropoff_datetime, VendorID,
        trip_distance, and total_amount.

        Args:
            row: pandas Series representing a row

        Returns:
            True if row exists, False otherwise
        """
        try:
            # Convert timestamps to strings for SQL
            pickup_dt = (
                row["tpep_pickup_datetime"].strftime("%Y-%m-%d %H:%M:%S")
                if hasattr(row["tpep_pickup_datetime"], "strftime")
                else str(row["tpep_pickup_datetime"])
            )
            dropoff_dt = (
                row["tpep_dropoff_datetime"].strftime("%Y-%m-%d %H:%M:%S")
                if hasattr(row["tpep_dropoff_datetime"], "strftime")
                else str(row["tpep_dropoff_datetime"])
            )

            # Build query to check for this exact row
            # Match on multiple columns to ensure it's the same row
            query = f"""
                SELECT COUNT(*) as count
                FROM `{STAGING_TABLE}`
                WHERE tpep_pickup_datetime = TIMESTAMP('{pickup_dt}')
                  AND tpep_dropoff_datetime = TIMESTAMP('{dropoff_dt}')
                  AND VendorID = {int(row['VendorID'])}
                  AND ABS(trip_distance - {float(row['trip_distance'])}) < 0.01
                  AND ABS(total_amount - {float(row['total_amount'])}) < 0.01
            """

            results = list(self.bq_client.execute_query(query))
            count = results[0].count if results else 0

            return count > 0

        except Exception as e:
            logger.error(f"Error checking if row exists: {e}")
            return False

    def is_staging_empty(self) -> bool:
        """
        Check if staging table is empty.

        Returns:
            True if staging is empty, False otherwise
        """
        try:
            count = self.bq_client.get_row_count(STAGING_TABLE)
            return count == 0
        except Exception as e:
            logger.error(f"Error checking if staging is empty: {e}")
            return True

    def download_parquet(self, month: int) -> Optional[pd.DataFrame]:
        """
        Download parquet file for a specific month using PyArrow for better performance.
        PyArrow is 3x faster and uses 30-40% less memory than pandas for large parquet files.

        Args:
            month: Month number (1-12)

        Returns:
            DataFrame with downloaded data or None if failed
        """
        try:
            url = get_parquet_url(month)
            month_name = get_month_name(month)

            logger.info(f"Downloading {month_name} {TARGET_YEAR} data from {url}")

            # Download with timeout
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            # Read parquet using PyArrow (3x faster than pandas for large files)
            parquet_file = pq.read_table(io.BytesIO(response.content))

            # Convert to pandas DataFrame
            df = parquet_file.to_pandas()

            logger.info(f"Downloaded {len(df):,} rows for {month_name} {TARGET_YEAR}")
            return df

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download data for month {month}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing parquet for month {month}: {e}")
            return None

    def download_all_months(self) -> int:
        """
        Download parquet files for all months and load each to staging immediately.
        Uses row-based checking to determine if data already exists.
        Checks complete rows (all columns) with min and max pickup_datetime.

        Returns:
            Total rows uploaded to staging (not including skipped months)
        """
        try:
            total_rows_uploaded = 0

            for month in range(1, 13):
                month_name = get_month_name(month)
                logger.info(f"Processing {month_name} 2024 ({month}/12)")

                # Download month
                logger.info(f"Downloading {month_name} 2024 data from source")
                df = self.download_parquet(month)
                if df is None:
                    logger.error(f"Failed to download month {month}, aborting")
                    raise Exception(f"Failed to download {month_name}")

                # Get data info
                incoming_rows = len(df)
                incoming_min = df["tpep_pickup_datetime"].min()
                incoming_max = df["tpep_pickup_datetime"].max()

                logger.info(
                    f"Downloaded data range: {incoming_min} to {incoming_max} ({incoming_rows:,} rows)"
                )

                # Check if this exact data already exists using row-based checking
                data_exists = self.check_if_data_exists_in_staging(df)

                if data_exists:
                    logger.info(
                        f"{month_name} data already exists in staging (verified by row matching), skipping upload"
                    )
                    del df
                    continue

                # Upload to staging
                logger.info(f"Loading {incoming_rows:,} rows to staging table")
                rows_loaded = self.load_to_staging(df)
                total_rows_uploaded += rows_loaded

                # Clear memory
                del df

                logger.info(
                    f"{month_name} uploaded. Total uploaded so far: {total_rows_uploaded:,} rows"
                )

            logger.info(
                f"All months processed. Total uploaded: {total_rows_uploaded:,} rows"
            )
            return total_rows_uploaded

        except Exception as e:
            logger.error(f"Error downloading all months: {e}")
            raise

    def load_to_staging(self, df: pd.DataFrame) -> int:
        """
        Load DataFrame to staging table.

        Args:
            df: DataFrame to load

        Returns:
            Number of rows loaded
        """
        try:
            logger.info(f"Loading {len(df):,} rows to staging table")
            rows_loaded = self.bq_client.load_dataframe_to_table(
                df, STAGING_TABLE, write_disposition="WRITE_APPEND"
            )
            logger.info(f"Successfully loaded {rows_loaded:,} rows to staging")
            return rows_loaded

        except Exception as e:
            logger.error(f"Failed to load to staging: {e}")
            raise

    def get_staging_row_count_2024(self) -> int:
        """
        Get row count of 2024 data in staging table.
        Uses date range filter instead of EXTRACT for better performance.

        Returns:
            Number of 2024 rows in staging
        """
        try:
            # First get total count
            total_count = self.bq_client.get_row_count(STAGING_TABLE)
            logger.info(f"Staging table total row count: {total_count:,}")

            # Then get 2024 count using date range
            where_clause = "tpep_pickup_datetime >= '2024-01-01' AND tpep_pickup_datetime < '2025-01-01'"
            count_2024 = self.bq_client.get_row_count(STAGING_TABLE, where_clause)
            logger.info(
                f"Staging table 2024 row count (date range filter): {count_2024:,}"
            )

            # Debug: Check for NULL datetimes
            null_query = f"SELECT COUNT(*) as null_count FROM `{STAGING_TABLE}` WHERE tpep_pickup_datetime IS NULL"
            null_results = list(self.bq_client.execute_query(null_query))
            if null_results:
                null_count = null_results[0].null_count
                logger.info(f"Staging table NULL datetime count: {null_count:,}")

            # Debug: Check datetime range
            range_query = f"""
                SELECT 
                    MIN(tpep_pickup_datetime) as min_dt,
                    MAX(tpep_pickup_datetime) as max_dt
                FROM `{STAGING_TABLE}`
                WHERE tpep_pickup_datetime IS NOT NULL
            """
            range_results = list(self.bq_client.execute_query(range_query))
            if range_results:
                min_dt = range_results[0].min_dt
                max_dt = range_results[0].max_dt
                logger.info(f"Staging datetime range: {min_dt} to {max_dt}")

            return count_2024

        except Exception as e:
            logger.error(f"Error getting staging 2024 row count: {e}")
            return 0

    def get_raw_row_count(self) -> int:
        """
        Get row count from raw table.

        Returns:
            Number of rows in raw
        """
        try:
            count = self.bq_client.get_row_count(RAW_TABLE)
            logger.debug(f"Raw table row count: {count:,}")
            return count
        except Exception as e:
            logger.error(f"Error getting raw row count: {e}")
            return 0

    def load_staging_to_raw_full(self) -> int:
        """
        Load all 2024 data from staging to raw (full refresh).
        Uses date range instead of EXTRACT for better performance.

        Note: With new CREATE OR REPLACE approach, this method is deprecated.
        Raw table is now created directly from staging using SQL.

        Returns:
            Number of rows loaded
        """
        try:
            logger.info("Loading full year from staging to raw")

            # Truncate raw table
            if self.bq_client.table_exists(RAW_TABLE):
                logger.info("Truncating raw table")
                self.bq_client.truncate_table(RAW_TABLE)

            # Insert 2024 data from staging to raw using date range
            query = f"""
                INSERT INTO `{RAW_TABLE}`
                SELECT *
                FROM `{STAGING_TABLE}`
                WHERE tpep_pickup_datetime >= '2024-01-01'
                  AND tpep_pickup_datetime < '2025-01-01'
            """

            logger.info("Executing INSERT from staging to raw with date range filter")
            rows_loaded = self.bq_client.execute_dml(query)
            logger.info(f"Loaded {rows_loaded:,} rows from staging to raw")

            return rows_loaded

        except Exception as e:
            logger.error(f"Failed to load staging to raw: {e}")
            raise

    def load_staging_to_raw_incremental(self, month: int) -> int:
        """
        Load specific month from staging to raw (incremental).

        Note: With new CREATE OR REPLACE approach, this method is deprecated.
        Raw table is now created directly from staging using SQL.

        Args:
            month: Month number (1-12)

        Returns:
            Number of rows loaded
        """
        try:
            month_name = get_month_name(month)
            logger.info(f"Loading {month_name} from staging to raw")

            # Calculate date range
            start_date = f"{TARGET_YEAR}-{month:02d}-01"

            if month == 12:
                end_date = f"{TARGET_YEAR + 1}-01-01"
            else:
                end_date = f"{TARGET_YEAR}-{month + 1:02d}-01"

            # Insert month's data from staging to raw
            query = f"""
                INSERT INTO `{RAW_TABLE}`
                SELECT *
                FROM `{STAGING_TABLE}`
                WHERE tpep_pickup_datetime >= '{start_date}'
                    AND tpep_pickup_datetime < '{end_date}'
            """

            rows_loaded = self.bq_client.execute_dml(query)
            logger.info(f"Loaded {rows_loaded:,} rows for {month_name}")

            return rows_loaded

        except Exception as e:
            logger.error(f"Failed to load month {month} to raw: {e}")
            raise

    def should_load_to_raw(self) -> bool:
        """
        Determine if raw table needs to be recreated based on idempotency check.
        Compares row counts between staging (2024 only) and raw.

        Returns:
            True if raw needs loading, False if already in sync
        """
        try:
            # Get raw table row count (free - metadata)
            raw_count = self.get_raw_row_count()

            if raw_count == 0:
                logger.info("Raw table is empty, needs loading")
                return True

            # Get 2024 row count from staging (costs ~$1-2)
            staging_2024_count = self.get_staging_row_count_2024()

            if staging_2024_count == raw_count:
                logger.info(
                    f"Raw table in sync ({raw_count:,} rows), skipping recreation"
                )
                return False

            logger.info(
                f"Raw needs update: staging 2024 has {staging_2024_count:,} rows, raw has {raw_count:,} rows"
            )
            return True

        except Exception as e:
            logger.error(f"Error checking if raw needs loading: {e}")
            # On error, assume we need to load
            return True

    def load_full_refresh_to_staging(self) -> int:
        """
        Full refresh: Download all months and load to staging.
        Intelligently skips months that already exist (handles interruptions).
        Loads each month incrementally to avoid memory issues.
        Uses PyArrow for 3x faster parquet reading.

        Returns:
            Number of rows loaded (includes already existing rows in count)
        """
        try:
            logger.info("Starting full refresh load to staging")

            # Download and load all months (skips already loaded ones automatically)
            total_rows = self.download_all_months()

            if total_rows == 0:
                raise Exception("No data was loaded")

            logger.info(f"Full refresh complete: {total_rows:,} total rows in staging")
            return total_rows

        except Exception as e:
            logger.error(f"Failed full refresh to staging: {e}")
            raise

    def load_incremental_to_staging(self, month: int) -> int:
        """
        Incremental: Download specific month and load to staging.
        Uses smart checking based on actual min/max datetime from downloaded data.
        Automatically skips if data already exists (handles re-runs).
        Uses PyArrow for 3x faster parquet reading.

        Args:
            month: Month number (1-12)

        Returns:
            Number of rows loaded (0 if skipped)
        """
        try:
            month_name = get_month_name(month)

            logger.info(f"Checking {month_name} data in staging")

            # Download month first to check actual date range
            logger.info(f"Downloading {month_name} 2024 data from source")
            df = self.download_parquet(month)

            if df is None:
                raise Exception(f"Failed to download {month_name} data")

            # Get data info
            incoming_rows = len(df)
            incoming_min = df["tpep_pickup_datetime"].min()
            incoming_max = df["tpep_pickup_datetime"].max()

            logger.info(
                f"Downloaded data range: {incoming_min} to {incoming_max} ({incoming_rows:,} rows)"
            )

            # Check if this exact data already exists using row-based checking
            data_exists = self.check_if_data_exists_in_staging(df)

            if data_exists:
                logger.info(
                    f"{month_name} data already exists in staging (verified by row matching), skipping upload"
                )
                del df
                return 0

            # Load to staging
            logger.info(f"Loading {incoming_rows:,} rows to staging table")
            rows_loaded = self.load_to_staging(df)

            del df
            return rows_loaded

        except Exception as e:
            logger.error(f"Failed incremental load for month {month}: {e}")
            raise


if __name__ == "__main__":
    # Test data loader
    from src.bigquery_client import BigQueryClient

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        bq_client = BigQueryClient()
        loader = DataLoader(bq_client)

        logger.info("DataLoader initialized")

        # Test staging check
        is_empty = loader.is_staging_empty()
        logger.info(f"Staging is empty: {is_empty}")

        # Test row counts
        staging_count = loader.get_staging_row_count_2024()
        raw_count = loader.get_raw_row_count()

        logger.info(f"Staging 2024 rows: {staging_count:,}")
        logger.info(f"Raw rows: {raw_count:,}")

    except Exception as e:
        logger.error(f"Error: {e}")
