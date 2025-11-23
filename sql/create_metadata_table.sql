-- Create Metadata Table for Pipeline Tracking
-- Records all pipeline runs with detailed metrics
-- Clustered by run_timestamp for optimal query performance

CREATE TABLE IF NOT EXISTS `nyc-taxi-pipeline-477912.nyc_taxi_dataset.pipeline_metadata` (
    pipeline_name STRING NOT NULL,           -- 'full_refresh' or 'incremental'
    date_range STRING NOT NULL,              -- e.g., '2024-01-01 to 2024-01-31'
    month_loaded STRING NOT NULL,            -- e.g., 'January' or 'full year'
    status STRING NOT NULL,                  -- 'SUCCESS', 'FAILED', 'SKIPPED'
    rows_loaded INT64,                       -- Number of rows loaded
    run_timestamp TIMESTAMP NOT NULL,        -- When the pipeline was executed
    runtime FLOAT64,                         -- Execution time in seconds
    error_message STRING                     -- Error details if failed
)
CLUSTER BY run_timestamp
OPTIONS(
    description="Metadata tracking for NYC Taxi data pipeline execution history - clustered by timestamp"
);
