#!/bin/bash
#
# NYC Taxi Pipeline - Cron Execution Script
# Purpose: Wrapper script for scheduled pipeline execution via cron
# Usage: Add to crontab for automated daily runs
# Example crontab entry: 0 2 * * * /path/to/run_pipeline_cron.sh

# Configuration
PROJECT_DIR="/home/prantonia/nyc-taxi-pipeline"
PYTHON_VENV="$PROJECT_DIR/venv"
LOG_DIR="$PROJECT_DIR/logs/cron"
PIPELINE_SCRIPT="$PROJECT_DIR/run_incremental.py"

# Email notification settings (optional)
ENABLE_EMAIL_NOTIFICATIONS=false
NOTIFICATION_EMAIL="patraffiah@gmail.com"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Generate log filename with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/pipeline_${TIMESTAMP}.log"
ERROR_LOG="$LOG_DIR/pipeline_${TIMESTAMP}_error.log"

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to send email notification
send_notification() {
    local subject="$1"
    local message="$2"
    
    if [ "$ENABLE_EMAIL_NOTIFICATIONS" = true ]; then
        echo "$message" | mail -s "$subject" "$NOTIFICATION_EMAIL"
    fi
}

# Start execution
log_message "========================================"
log_message "NYC Taxi Pipeline - Cron Execution"
log_message "========================================"
log_message "Pipeline started"
log_message "Log file: $LOG_FILE"
log_message "Working directory: $PROJECT_DIR"
log_message ""

# Change to project directory
cd "$PROJECT_DIR" || {
    log_message "ERROR: Failed to change to project directory: $PROJECT_DIR"
    send_notification "NYC Taxi Pipeline FAILED" "Could not change to project directory"
    exit 1
}

# Check if virtual environment exists
if [ ! -d "$PYTHON_VENV" ]; then
    log_message "ERROR: Virtual environment not found at $PYTHON_VENV"
    send_notification "NYC Taxi Pipeline FAILED" "Virtual environment not found"
    exit 1
fi

# Activate virtual environment
log_message "Activating virtual environment..."
source "$PYTHON_VENV/bin/activate" || {
    log_message "ERROR: Failed to activate virtual environment"
    send_notification "NYC Taxi Pipeline FAILED" "Could not activate virtual environment"
    exit 1
}

# Verify Python and dependencies
log_message "Verifying Python environment..."
python --version >> "$LOG_FILE" 2>&1
pip list >> "$LOG_FILE" 2>&1

# Check if .env file exists
if [ ! -f "$PROJECT_DIR/.env" ]; then
    log_message "WARNING: .env file not found. Using environment variables."
fi

# Run the pipeline
log_message ""
log_message "Starting pipeline execution..."
log_message "=========================================="

python "$PIPELINE_SCRIPT" >> "$LOG_FILE" 2>> "$ERROR_LOG"
EXIT_CODE=$?

log_message "=========================================="
log_message "Pipeline execution completed"
log_message "Exit code: $EXIT_CODE"

# Check exit code and send notification
if [ $EXIT_CODE -eq 0 ]; then
    log_message " Pipeline completed successfully"
    log_message ""
    
    # Extract and log summary from metadata using Python logging
    log_message "Extracting pipeline summary..."
    python - <<END >> "$LOG_FILE" 2>&1
import logging
from src.bigquery_client import BigQueryClient
from src.metadata_manager import MetadataManager

# Configure logging to append to same log file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
)

try:
    client = BigQueryClient()
    metadata = MetadataManager(client)
    last_run = metadata.get_last_successful_run('incremental')
    
    if last_run:
        logging.info(f"Last successful run: {last_run['run_timestamp']}")
        logging.info(f"Month loaded: {last_run['month_loaded']}")
        logging.info(f"Rows loaded: {last_run['rows_loaded']:,}")
        logging.info(f"Runtime: {last_run['runtime']:.2f} seconds")
    
    client.close()
except Exception as e:
    logging.error(f"Could not retrieve metadata: {e}")
END
    
    send_notification "NYC Taxi Pipeline SUCCESS" "Pipeline completed successfully. See log: $LOG_FILE"
else
    log_message " Pipeline failed with exit code $EXIT_CODE"
    log_message ""
    log_message "Error details:"
    cat "$ERROR_LOG" >> "$LOG_FILE"
    
    send_notification "NYC Taxi Pipeline FAILED" "Pipeline failed with exit code $EXIT_CODE. See log: $LOG_FILE"
fi

# Deactivate virtual environment
deactivate

# Cleanup old logs (keep last 30 days)
log_message ""
log_message "Cleaning up old log files (keeping last 30 days)..."
find "$LOG_DIR" -name "pipeline_*.log" -mtime +30 -delete
find "$LOG_DIR" -name "pipeline_*_error.log" -mtime +30 -delete
log_message "Cleanup completed"

log_message ""
log_message "========================================"
log_message "Cron execution finished"
log_message "========================================"

exit $EXIT_CODE
