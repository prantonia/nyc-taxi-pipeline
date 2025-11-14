"""
Configuration module for NYC Taxi Data Pipeline.
Contains all project settings, GCP configurations, and constants.
"""
import os
import logging
from dotenv import load_dotenv

# Setup logger
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# GCP Configuration - MUST be set in .env file
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BQ_DATASET')
CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Validate required environment variables
if not PROJECT_ID:
    raise ValueError("GCP_PROJECT_ID must be set in .env file")
if not DATASET_ID:
    raise ValueError("BQ_DATASET must be set in .env file")
if not CREDENTIALS_PATH:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS must be set in .env file")

# Table Names - MUST be set in .env file
STAGING_TABLE_NAME = os.getenv('STAGING_TABLE_NAME')
RAW_TABLE_NAME = os.getenv('RAW_TABLE_NAME')
SILVER_TABLE_NAME = os.getenv('SILVER_TABLE_NAME')
GOLD_TABLE_NAME = os.getenv('GOLD_TABLE_NAME')
METADATA_TABLE_NAME = os.getenv('METADATA_TABLE_NAME')

# Validate table names
if not STAGING_TABLE_NAME:
    raise ValueError("STAGING_TABLE_NAME must be set in .env file")
if not RAW_TABLE_NAME:
    raise ValueError("RAW_TABLE_NAME must be set in .env file")
if not SILVER_TABLE_NAME:
    raise ValueError("SILVER_TABLE_NAME must be set in .env file")
if not GOLD_TABLE_NAME:
    raise ValueError("GOLD_TABLE_NAME must be set in .env file")
if not METADATA_TABLE_NAME:
    raise ValueError("METADATA_TABLE_NAME must be set in .env file")

# Build fully qualified table names
STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_NAME}"
RAW_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE_NAME}"
SILVER_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{SILVER_TABLE_NAME}"
GOLD_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{GOLD_TABLE_NAME}"
METADATA_TABLE = f"{PROJECT_ID}.{DATASET_ID}.{METADATA_TABLE_NAME}"

# Data Source Configuration
NYC_TAXI_BASE_URL = os.getenv('NYC_TAXI_BASE_URL')
TAXI_FILE_TEMPLATE = os.getenv('TAXI_FILE_TEMPLATE')

# Validate data source configuration
if not NYC_TAXI_BASE_URL:
    raise ValueError("NYC_TAXI_BASE_URL must be set in .env file")
if not TAXI_FILE_TEMPLATE:
    raise ValueError("TAXI_FILE_TEMPLATE must be set in .env file")

# Year Configuration
TARGET_YEAR = 2024
MONTHS = list(range(1, 13))  # 1 to 12

# Logging Configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'logs/pipeline.log')
LOG_FORMAT = '%(asctime)s | %(levelname)s | %(name)s | %(message)s'

# Retry Configuration
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # seconds

# Pipeline Types
PIPELINE_FULL_REFRESH = 'full_refresh'
PIPELINE_INCREMENTAL = 'incremental'

# Status Constants
STATUS_SUCCESS = 'SUCCESS'
STATUS_FAILED = 'FAILED'
STATUS_SKIPPED = 'SKIPPED'

# Date Formats
DATE_FORMAT = '%Y-%m-%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

# Month Names
MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

# SQL Files Directory
SQL_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql')


def get_parquet_url(month: int) -> str:
    """Generate the URL for a specific month's parquet file."""
    filename = TAXI_FILE_TEMPLATE.format(month=month)
    return f"{NYC_TAXI_BASE_URL}/{filename}"


def get_month_name(month: int) -> str:
    """Get month name from month number."""
    return MONTH_NAMES.get(month, f"Month-{month}")


def get_date_range_string(month: int = None) -> str:
    """
    Get date range string for metadata.
    
    Args:
        month: Month number (1-12), None for full year
        
    Returns:
        Date range string (e.g., "2024-01-01 to 2024-01-31" or "2024-01-01 - 2024-12-31")
    """
    if month is None:
        return f"{TARGET_YEAR}-01-01 - {TARGET_YEAR}-12-31"
    
    # Calculate last day of month
    if month == 12:
        next_month_year = TARGET_YEAR + 1
        next_month = 1
    else:
        next_month_year = TARGET_YEAR
        next_month = month + 1
    
    from datetime import datetime, timedelta
    last_day = (datetime(next_month_year, next_month, 1) - timedelta(days=1)).day
    
    return f"{TARGET_YEAR}-{month:02d}-01 to {TARGET_YEAR}-{month:02d}-{last_day}"


def get_month_loaded_string(month: int = None) -> str:
    """
    Get month loaded string for metadata.
    
    Args:
        month: Month number (1-12), None for full year
        
    Returns:
        Month loaded string (e.g., "January" or "full year")
    """
    if month is None:
        return "full year"
    return get_month_name(month)


# Validation
def validate_config():
    """Validate that all required configuration is present."""
    errors = []
    
    if not PROJECT_ID:
        errors.append("GCP_PROJECT_ID is not set")
    
    if not DATASET_ID:
        errors.append("BQ_DATASET is not set")
    
    if not os.path.exists(CREDENTIALS_PATH):
        errors.append(f"Service account file not found: {CREDENTIALS_PATH}")
    
    if errors:
        raise ValueError(f"Configuration errors: {', '.join(errors)}")
    
    return True


if __name__ == "__main__":
    # Test configuration
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT
    )
    
    logger.info("NYC Taxi Pipeline Configuration")
    logger.info("=" * 50)
    logger.info(f"Project ID: {PROJECT_ID}")
    logger.info(f"Dataset ID: {DATASET_ID}")
    logger.info(f"Staging Table: {STAGING_TABLE}")
    logger.info(f"Raw Table: {RAW_TABLE}")
    logger.info(f"Silver Table: {SILVER_TABLE}")
    logger.info(f"Gold Table: {GOLD_TABLE}")
    logger.info(f"Metadata Table: {METADATA_TABLE}")
    logger.info(f"Credentials Path: {CREDENTIALS_PATH}")
    logger.info(f"Target Year: {TARGET_YEAR}")
    logger.info("=" * 50)
    
    try:
        validate_config()
        logger.info("Configuration is valid")
    except ValueError as e:
        logger.error(f"Configuration error: {e}")