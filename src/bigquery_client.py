"""
BigQuery client module for database operations.
Handles all interactions with Google BigQuery.
Uses PyArrow for optimized DataFrame operations.
"""
import logging
from typing import Optional
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
import pandas as pd

from src.config import PROJECT_ID, CREDENTIALS_PATH

# Setup logger
logger = logging.getLogger(__name__)


class BigQueryClient:
    """Wrapper class for BigQuery operations."""

    def __init__(self):
        """Initialize BigQuery client."""
        try:
            self.client = bigquery.Client(
                project=PROJECT_ID, credentials=self._get_credentials()
            )
            logger.info(f"BigQuery client initialized for project: {PROJECT_ID}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery client: {e}")
            raise

    def _get_credentials(self):
        """Load credentials from service account file."""
        from google.oauth2 import service_account

        try:
            credentials = service_account.Credentials.from_service_account_file(
                CREDENTIALS_PATH, scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            return credentials
        except Exception as e:
            logger.error(f"Failed to load credentials from {CREDENTIALS_PATH}: {e}")
            raise

    def execute_query(
        self, query: str, job_config: Optional[bigquery.QueryJobConfig] = None
    ) -> bigquery.table.RowIterator:
        """
        Execute a SQL query and return results.

        Args:
            query: SQL query string
            job_config: Optional query job configuration

        Returns:
            Query results as RowIterator
        """
        try:
            logger.debug(f"Executing query: {query[:200]}...")
            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()
            logger.info(
                f"Query executed successfully. Rows processed: {query_job.total_bytes_processed}"
            )
            return results
        except GoogleCloudError as e:
            logger.error(f"BigQuery error executing query: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            raise

    def execute_dml(self, query: str) -> int:
        """
        Execute a DML query (INSERT, UPDATE, DELETE) and return affected rows.

        Args:
            query: DML SQL query string

        Returns:
            Number of affected rows
        """
        try:
            logger.debug(f"Executing DML: {query[:200]}...")
            query_job = self.client.query(query)
            query_job.result()
            affected_rows = query_job.num_dml_affected_rows
            logger.info(f"DML executed successfully. Affected rows: {affected_rows}")
            return affected_rows
        except GoogleCloudError as e:
            logger.error(f"BigQuery error executing DML: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing DML: {e}")
            raise

    def execute_ddl(self, query: str) -> None:
        """
        Execute a DDL query (CREATE, DROP, ALTER).

        Args:
            query: DDL SQL query string
        """
        try:
            logger.debug(f"Executing DDL: {query[:200]}...")
            query_job = self.client.query(query)
            query_job.result()
            logger.info("DDL executed successfully")
        except GoogleCloudError as e:
            logger.error(f"BigQuery error executing DDL: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing DDL: {e}")
            raise

    def table_exists(self, table_id: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_id: Fully qualified table ID (project.dataset.table)

        Returns:
            True if table exists, False otherwise
        """
        try:
            self.client.get_table(table_id)
            logger.debug(f"Table exists: {table_id}")
            return True
        except Exception:
            logger.debug(f"Table does not exist: {table_id}")
            return False

    def get_row_count(self, table_id: str, where_clause: str = "") -> int:
        """
        Get row count from a table.

        Args:
            table_id: Fully qualified table ID
            where_clause: Optional WHERE clause (without WHERE keyword)

        Returns:
            Number of rows
        """
        try:
            where_sql = f"WHERE {where_clause}" if where_clause else ""
            query = f"SELECT COUNT(*) as count FROM `{table_id}` {where_sql}"
            results = self.execute_query(query)
            count = list(results)[0].count
            logger.debug(f"Row count for {table_id}: {count}")
            return count
        except Exception as e:
            logger.error(f"Error getting row count for {table_id}: {e}")
            raise

    def get_query_result_as_dataframe(self, query: str) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.

        Args:
            query: SQL query string

        Returns:
            Query results as DataFrame
        """
        try:
            logger.debug(f"Executing query for DataFrame: {query[:200]}...")
            df = self.client.query(query).to_dataframe()
            logger.info(f"Query returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error getting query results as DataFrame: {e}")
            raise

    def load_dataframe_to_table(
        self, df: pd.DataFrame, table_id: str, write_disposition: str = "WRITE_APPEND"
    ) -> int:
        """
        Load pandas DataFrame to BigQuery table.
        When PyArrow is installed, BigQuery automatically uses it for better performance.

        Args:
            df: DataFrame to load
            table_id: Fully qualified table ID
            write_disposition: WRITE_APPEND, WRITE_TRUNCATE, or WRITE_EMPTY

        Returns:
            Number of rows loaded
        """
        try:
            logger.info(f"Loading {len(df)} rows to {table_id}")

            # BigQuery automatically uses PyArrow if installed (no explicit config needed)
            job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()

            logger.info(f"Successfully loaded {len(df)} rows to {table_id}")
            return len(df)

        except Exception as e:
            logger.error(f"Error loading DataFrame to {table_id}: {e}")
            raise

    def get_min_max_datetime(
        self, table_id: str, datetime_column: str, where_clause: str = ""
    ) -> tuple:
        """
        Get minimum and maximum datetime from a column.

        Args:
            table_id: Fully qualified table ID
            datetime_column: Name of datetime column
            where_clause: Optional WHERE clause

        Returns:
            Tuple of (min_datetime, max_datetime)
        """
        try:
            where_sql = f"WHERE {where_clause}" if where_clause else ""
            query = f"""
                SELECT 
                    MIN({datetime_column}) as min_dt,
                    MAX({datetime_column}) as max_dt
                FROM `{table_id}`
                {where_sql}
            """
            results = list(self.execute_query(query))
            if results:
                row = results[0]
                return (row.min_dt, row.max_dt)
            return (None, None)
        except Exception as e:
            logger.error(f"Error getting min/max datetime: {e}")
            raise

    def truncate_table(self, table_id: str) -> None:
        """
        Truncate (delete all rows from) a table.

        Args:
            table_id: Fully qualified table ID
        """
        try:
            query = f"TRUNCATE TABLE `{table_id}`"
            self.execute_ddl(query)
            logger.info(f"Table truncated: {table_id}")
        except Exception as e:
            logger.error(f"Error truncating table {table_id}: {e}")
            raise

    def drop_table(self, table_id: str) -> None:
        """
        Drop a table if it exists.

        Args:
            table_id: Fully qualified table ID
        """
        try:
            query = f"DROP TABLE IF EXISTS `{table_id}`"
            self.execute_ddl(query)
            logger.info(f"Table dropped: {table_id}")
        except Exception as e:
            logger.error(f"Error dropping table {table_id}: {e}")
            raise

    def execute_sql_file(self, filepath: str) -> None:
        """
        Execute SQL from a file.

        Args:
            filepath: Path to SQL file
        """
        try:
            with open(filepath, "r") as f:
                sql = f.read()

            # Split by semicolons for multiple statements
            statements = [s.strip() for s in sql.split(";") if s.strip()]

            for statement in statements:
                if statement:
                    logger.debug(f"Executing statement from {filepath}")
                    self.execute_ddl(statement)

            logger.info(f"Successfully executed SQL file: {filepath}")
        except Exception as e:
            logger.error(f"Error executing SQL file {filepath}: {e}")
            raise

    def close(self):
        """Close the BigQuery client connection."""
        try:
            self.client.close()
            logger.info("BigQuery client closed")
        except Exception as e:
            logger.warning(f"Error closing BigQuery client: {e}")


if __name__ == "__main__":
    # Test BigQuery client
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    try:
        client = BigQueryClient()
        logger.info("BigQuery client initialized successfully")

        # Test simple query
        query = "SELECT 1 as test"
        results = client.execute_query(query)
        logger.info(f"Test query executed: {list(results)}")

        client.close()
    except Exception as e:
        logger.error(f"Error: {e}")
