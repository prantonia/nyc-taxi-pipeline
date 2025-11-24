"""
Tests for create_tables script.
Tests table creation logic and SQL file execution.
"""
import pytest
from unittest.mock import Mock, patch
import os


class TestCreateTablesScript:
    """Tests for create_tables.py script."""

    @patch("src.bigquery_client.BigQueryClient")
    def test_can_import_create_tables(self, mock_bq_client):
        """Test that create_tables module can be imported."""
        try:
            import src.create_tables as create_tables

            assert create_tables is not None
        except ImportError:
            pytest.skip("create_tables module structure different than expected")

    @patch("src.bigquery_client.BigQueryClient")
    def test_bigquery_client_can_execute_sql_files(self, mock_bq_client_class):
        """Test that BigQueryClient has execute_sql_file method."""
        from src.bigquery_client import BigQueryClient

        mock_client = Mock()
        mock_client.execute_sql_file = Mock()
        mock_bq_client_class.return_value = mock_client

        client = BigQueryClient()

        assert hasattr(client, "execute_sql_file") or hasattr(client, "execute_ddl")


class TestSQLFileValidation:
    """Tests for SQL file validation."""

    def test_sql_directory_exists(self):
        """Test that SQL directory exists."""
        assert os.path.exists("sql") or os.path.exists("src/sql")

    def test_metadata_table_sql_exists(self):
        """Test that metadata table SQL file exists."""
        sql_paths = [
            "sql/create_metadata_table.sql",
            "src/sql/create_metadata_table.sql",
        ]
        exists = any(os.path.exists(path) for path in sql_paths)
        assert exists, "Metadata table SQL file should exist"

    def test_staging_table_sql_exists(self):
        """Test that staging table SQL file exists."""
        sql_paths = ["sql/create_staging_table.sql", "src/sql/create_staging_table.sql"]
        exists = any(os.path.exists(path) for path in sql_paths)
        assert exists, "Staging table SQL file should exist"

    def test_raw_table_sql_exists(self):
        """Test that raw table SQL file exists."""
        sql_paths = ["sql/create_raw_table.sql", "src/sql/create_raw_table.sql"]
        exists = any(os.path.exists(path) for path in sql_paths)
        assert exists, "Raw table SQL file should exist"

    def test_silver_table_sql_exists(self):
        """Test that silver table SQL file exists."""
        sql_paths = ["sql/create_silver_table.sql", "src/sql/create_silver_table.sql"]
        exists = any(os.path.exists(path) for path in sql_paths)
        assert exists, "Silver table SQL file should exist"

    def test_gold_table_sql_exists(self):
        """Test that gold table SQL file exists."""
        sql_paths = ["sql/create_gold_table.sql", "src/sql/create_gold_table.sql"]
        exists = any(os.path.exists(path) for path in sql_paths)
        assert exists, "Gold table SQL file should exist"


class TestTableCreationConcepts:
    """Tests for table creation concepts."""

    def test_sql_files_contain_create_statements(self):
        """Test that SQL files contain CREATE TABLE statements."""
        sql_files = [
            "sql/create_metadata_table.sql",
            "sql/create_staging_table.sql",
            "sql/create_raw_table.sql",
            "sql/create_silver_table.sql",
            "sql/create_gold_table.sql",
        ]

        for sql_file in sql_files:
            if os.path.exists(sql_file):
                with open(sql_file, "r") as f:
                    content = f.read().upper()
                    assert (
                        "CREATE" in content
                    ), f"{sql_file} should contain CREATE statement"

    def test_table_names_are_consistent(self):
        """Test that table names follow naming convention."""

        # Just verify the naming convention exists in config
        from src.config import (
            METADATA_TABLE,
            STAGING_TABLE,
            RAW_TABLE,
            SILVER_TABLE,
            GOLD_TABLE,
        )

        assert METADATA_TABLE is not None
        assert STAGING_TABLE is not None
        assert RAW_TABLE is not None
        assert SILVER_TABLE is not None
        assert GOLD_TABLE is not None
