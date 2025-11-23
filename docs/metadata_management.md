# Metadata Management Process

## **Overview**

This document explains how the NYC Taxi Data Pipeline tracks, manages, and uses metadata to ensure reliable execution and enable audit trails.

---

## **Purpose of Metadata**

The metadata system serves four critical functions:

1. **Execution Tracking** - Record every pipeline run
2. **State Management** - Determine what to load next
3. **Audit Trail** - Complete history for compliance
4. **Performance Monitoring** - Track pipeline metrics

---

## **Metadata Table Schema**

### **Table Structure**

```sql
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    pipeline_name STRING NOT NULL,      -- 'full_refresh' or 'incremental'
    date_range STRING NOT NULL,         -- '2024-01-01 to 2024-01-31'
    month_loaded STRING NOT NULL,       -- 'January' or 'full year'
    status STRING NOT NULL,             -- 'SUCCESS', 'FAILED', 'SKIPPED'
    rows_loaded INT64 NOT NULL,         -- Number of rows processed
    run_timestamp TIMESTAMP NOT NULL,   -- When pipeline ran
    runtime FLOAT64 NOT NULL,           -- Execution time in seconds
    error_message STRING                -- Error details if failed
);
```

### **Column Descriptions**

| Column | Type | Purpose | Example Values |
|--------|------|---------|----------------|
| `pipeline_name` | STRING | Identifies pipeline mode | `incremental`, `full_refresh` |
| `date_range` | STRING | Date range of data processed | `2024-01-01 to 2024-01-31` |
| `month_loaded` | STRING | Human-readable month | `January`, `full year` |
| `status` | STRING | Run outcome | `SUCCESS`, `FAILED`, `SKIPPED` |
| `rows_loaded` | INT64 | Rows added to raw layer | `2964609`, `0` (if skipped) |
| `run_timestamp` | TIMESTAMP | Execution time | `2024-11-13 10:30:00 UTC` |
| `runtime` | FLOAT64 | Duration in seconds | `1245.67` |
| `error_message` | STRING | Error if failed | `Network timeout` or `NULL` |

---

## **Recording Pipeline Runs**

### **MetadataManager Class**

```python
class MetadataManager:
    """Manager for pipeline metadata tracking."""
    
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
            pipeline_name: 'full_refresh' or 'incremental'
            status: 'SUCCESS', 'FAILED', or 'SKIPPED'
            rows_loaded: Number of rows loaded
            month: Month number (1-12) or None for full refresh
            runtime: Execution time in seconds
            error_message: Error details if failed
        """
```

### **Example Usage**

#### **Successful Run**

```python
# Record successful incremental load
metadata.record_run(
    pipeline_name="incremental",
    status="SUCCESS",
    rows_loaded=2964609,
    month=1,  # January
    runtime=245.67
)
```

**Metadata Entry:**
```
pipeline_name: incremental
date_range: 2024-01-01 to 2024-01-31
month_loaded: January
status: SUCCESS
rows_loaded: 2964609
run_timestamp: 2024-11-13 10:30:00
runtime: 245.67
error_message: NULL
```

#### **Skipped Run**

```python
# Record skipped run (data already exists)
metadata.record_run(
    pipeline_name="incremental",
    status="SKIPPED",
    rows_loaded=0,
    month=2,  # February
    runtime=5.23
)
```

**Metadata Entry:**
```
pipeline_name: incremental
date_range: 2024-02-01 to 2024-02-29
month_loaded: February
status: SKIPPED
rows_loaded: 0
run_timestamp: 2024-11-13 10:35:23
runtime: 5.23
error_message: NULL
```

#### **Failed Run**

```python
# Record failed run
metadata.record_run(
    pipeline_name="incremental",
    status="FAILED",
    rows_loaded=0,
    month=3,  # March
    runtime=89.45,
    error_message="Network timeout after 60 seconds"
)
```

**Metadata Entry:**
```
pipeline_name: incremental
date_range: 2024-03-01 to 2024-03-31
month_loaded: March
status: FAILED
rows_loaded: 0
run_timestamp: 2024-11-13 10:37:52
runtime: 89.45
error_message: Network timeout after 60 seconds
```

---

## **Querying Metadata**

### **Key Queries**

#### **1. Get Last Completed Month**

```python
def get_last_completed_month(self, pipeline_name='incremental'):
    """
    Get the last month that completed (SUCCESS or SKIPPED).
    Critical for auto-progression.
    """
    query = f"""
        SELECT month_loaded
        FROM pipeline_metadata
        WHERE pipeline_name = '{pipeline_name}'
          AND (status = 'SUCCESS' OR status = 'SKIPPED')
          AND month_loaded != 'full year'
        ORDER BY run_timestamp ASC
        LIMIT 1
    """
```

**Usage:**
```python
last_month = metadata.get_last_completed_month()
# Returns: 7 (July)

next_month = last_month + 1
# Next month to load: 8 (August)
```

#### **2. Check if Full Year Loaded**

```python
def is_full_year_loaded(self):
    """
    Check if all 12 months have been loaded.
    """
    query = """
        SELECT COUNT(DISTINCT month_loaded) as count
        FROM pipeline_metadata
        WHERE pipeline_name = 'incremental'
          AND (status = 'SUCCESS' OR status = 'SKIPPED')
          AND month_loaded != 'full year'
    """
    # Returns: True if count == 12
```

**Usage:**
```python
if metadata.is_full_year_loaded():
    logger.info("✓ All months loaded, pipeline complete")
    return
```

#### **3. Get Run History**

```python
def get_run_history(self, limit=10):
    """
    Get recent pipeline runs for monitoring.
    """
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
        FROM pipeline_metadata
        ORDER BY run_timestamp ASC
        LIMIT {limit}
    """
```

**Usage:**
```python
history = metadata.get_run_history(limit=5)
for run in history:
    print(f"{run['month_loaded']}: {run['status']} ({run['rows_loaded']:,} rows)")
```

---

## **Status State Machine**

### **Status Values and Meanings**

```
┌──────────────┐
│   RUNNING    │ (Not stored in metadata)
└──────┬───────┘
       │
       ├──────────────┬──────────────┐
       │              │              │
       ▼              ▼              ▼
┌──────────┐   ┌──────────┐   ┌──────────┐
│ SUCCESS  │   │ SKIPPED  │   │  FAILED  │
└──────────┘   └──────────┘   └──────────┘
     │              │              │
     └──────────────┴──────────────┘
                    │
                    ▼
            Next month or Done
```

### **Status Decision Logic**

```python
def determine_status(raw_rows_loaded):
    """
    Determine pipeline status based on outcome.
    """
    if raw_rows_loaded > 0:
        return STATUS_SUCCESS  # New data was loaded
    else:
        return STATUS_SKIPPED  # Data already existed
```

### **Status Examples**

| Scenario | Staging Status | Raw Status | Final Status | Rows Loaded |
|----------|---------------|------------|--------------|-------------|
| New month, new data | Downloaded | Loaded | SUCCESS | 2,964,609 |
| Month exists in staging | Skipped | Loaded | SUCCESS | 2,964,609 |
| Month exists everywhere | Skipped | Skipped | SKIPPED | 0 |
| Network error | Failed | N/A | FAILED | 0 |

---

## **Auto-Progression Logic**

### **How Pipeline Determines Next Month**

```python
def _get_next_month_to_load(self):
    """
    Intelligent month selection based on metadata.
    """
    # Step 1: Get last completed month
    last_month = self.metadata.get_last_completed_month()
    
    # Step 2: Determine next month
    if last_month is None:
        # No history - start with January
        return 1
    
    elif last_month == 12:
        # All months done
        return None
    
    else:
        # Load next month
        return last_month + 1
```

### **Progression Examples**

#### **Scenario 1: Fresh Start**

```
Metadata Table: (empty)

Pipeline determines: Load month 1 (January)
```

#### **Scenario 2: Mid-Year**

```
Metadata Table:
- January: SUCCESS
- February: SUCCESS  
- March: SUCCESS
- April: SKIPPED
- May: SUCCESS
- June: SUCCESS
- July: SKIPPED  ← Last completed

Pipeline determines: Load month 8 (August)
```

#### **Scenario 3: Complete Year**

```
Metadata Table:
- January through December: All SUCCESS or SKIPPED

Pipeline determines: None (all months loaded)
```

---

## **Performance Metrics**

### **Calculating Metrics from Metadata**

#### **Average Pipeline Runtime**

```sql
SELECT 
    pipeline_name,
    AVG(runtime) as avg_runtime_seconds,
    MIN(runtime) as min_runtime,
    MAX(runtime) as max_runtime
FROM pipeline_metadata
WHERE status = 'SUCCESS'
GROUP BY pipeline_name;
```

#### **Success Rate**

```sql
SELECT 
    pipeline_name,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
    ROUND(100.0 * SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM pipeline_metadata
GROUP BY pipeline_name;
```

#### **Rows Per Month**

```sql
SELECT 
    month_loaded,
    SUM(rows_loaded) as total_rows,
    COUNT(*) as run_count
FROM pipeline_metadata
WHERE status = 'SUCCESS'
  AND month_loaded != 'full year'
GROUP BY month_loaded
ORDER BY month_loaded;
```

#### **Failure Analysis**

```sql
SELECT 
    month_loaded,
    error_message,
    COUNT(*) as failure_count,
    MAX(run_timestamp) as last_failure
FROM pipeline_metadata
WHERE status = 'FAILED'
GROUP BY month_loaded, error_message
ORDER BY failure_count DESC;
```

---

## **Metadata-Driven Debugging**

### **Common Debugging Scenarios**

#### **1. Pipeline Stuck on One Month**

**Symptom:** Pipeline keeps trying to load July

**Diagnosis:**
```sql
SELECT *
FROM pipeline_metadata
WHERE month_loaded = 'July'
ORDER BY run_timestamp DESC
LIMIT 5;
```

**Possible causes:**
```
All runs show: status = 'FAILED'
→ Fix the underlying error

All runs show: status = 'SKIPPED'  
→ Check auto-progression logic (should move to August)
```

#### **2. Missing Months**

**Symptom:** Some months never loaded

**Diagnosis:**
```sql
WITH all_months AS (
    SELECT 'January' as month UNION ALL
    SELECT 'February' UNION ALL
    SELECT 'March' UNION ALL
    SELECT 'April' UNION ALL
    SELECT 'May' UNION ALL
    SELECT 'June' UNION ALL
    SELECT 'July' UNION ALL
    SELECT 'August' UNION ALL
    SELECT 'September' UNION ALL
    SELECT 'October' UNION ALL
    SELECT 'November' UNION ALL
    SELECT 'December'
)
SELECT 
    am.month,
    COALESCE(m.status, 'NOT LOADED') as status
FROM all_months am
LEFT JOIN (
    SELECT DISTINCT month_loaded, status
    FROM pipeline_metadata
    WHERE status IN ('SUCCESS', 'SKIPPED')
) m ON am.month = m.month_loaded;
```

#### **3. Performance Degradation**

**Symptom:** Pipeline getting slower over time

**Diagnosis:**
```sql
SELECT 
    DATE(run_timestamp) as run_date,
    AVG(runtime) as avg_runtime,
    AVG(rows_loaded) as avg_rows
FROM pipeline_metadata
WHERE status = 'SUCCESS'
GROUP BY DATE(run_timestamp)
ORDER BY run_date;
```

---

## **Data Integrity**

### **Ensuring Metadata Accuracy**

#### **Transaction Safety**

```python
def record_run(self, ...):
    """
    Metadata recording is fail-safe.
    Even if metadata insert fails, pipeline status is logged.
    """
    try:
        # Insert metadata
        self.bq_client.execute_dml(insert_query)
        logger.info("Metadata recorded successfully")
    except Exception as e:
        logger.error(f"Failed to record metadata: {e}")
        # Don't raise - metadata failure shouldn't stop pipeline
```

#### **Idempotent Recording**

Metadata can be recorded multiple times without issues:

```python
# Multiple runs of same month create separate records
# Each record has unique run_timestamp
# Latest record (ORDER BY run_timestamp DESC) is used
```

---

## **Metadata Reporting**

### **Log Formatted History**

```python
def log_run_history(self, limit=10):
    """
    Print formatted run history to logs.
    """
    history = self.get_run_history(limit)
    
    logger.info("=" * 100)
    logger.info(f"Pipeline Run History (Last {limit} runs)")
    logger.info("=" * 100)
    
    for run in history:
        logger.info("")
        logger.info(f"Pipeline: {run['pipeline_name']}")
        logger.info(f"Month: {run['month_loaded']}")
        logger.info(f"Status: {run['status']}")
        logger.info(f"Rows: {run['rows_loaded']:,}")
        logger.info(f"Runtime: {run['runtime']:.2f}s")
        logger.info(f"Timestamp: {run['run_timestamp']}")
        if run['error_message']:
            logger.info(f"Error: {run['error_message']}")
        logger.info("-" * 100)
```

**Output:**
```
================================================================================
Pipeline Run History (Last 10 runs)
================================================================================

Pipeline: incremental
Month: July
Status: SKIPPED
Rows: 0
Runtime: 5.23s
Timestamp: 2024-11-13 10:45:00
--------------------------------------------------------------------------------

Pipeline: incremental
Month: June
Status: SUCCESS
Rows: 2,847,123
Runtime: 245.89s
Timestamp: 2024-11-13 10:40:00
--------------------------------------------------------------------------------
```

---

## **Metadata Maintenance**

### **Cleanup Old Records**

```sql
-- Delete records older than 90 days
DELETE FROM pipeline_metadata
WHERE run_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY);
```

### **Archive Historical Data**

```sql
-- Move old records to archive table
CREATE TABLE pipeline_metadata_archive AS
SELECT *
FROM pipeline_metadata
WHERE run_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY);

-- Then delete from main table
DELETE FROM pipeline_metadata
WHERE run_timestamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY);
```

---

## **Best Practices**

### **1. Always Record Runs**

```python
# Good: Always record outcome
try:
    result = run_pipeline()
    metadata.record_run(status=SUCCESS, rows=result)
except Exception as e:
    metadata.record_run(status=FAILED, error=str(e))

# Bad: Missing metadata
run_pipeline()  # No record of execution!
```

### **2. Use Metadata for Decisions**

```python
# Good: Let metadata drive logic
next_month = metadata.get_last_completed_month() + 1

# Bad: Hardcoded logic
next_month = 5  # What if we already loaded June?
```

### **3. Include Sufficient Detail**

```python
# Good: Detailed error message
error_message = f"Network timeout after {timeout}s connecting to {url}"

# Bad: Vague error
error_message = "Failed"
```

---

## **Summary**

The metadata system provides:

- **Complete audit trail** - Every run recorded
- **Automatic progression** - Pipeline knows what to do next
- **Performance monitoring** - Track runtime and efficiency
- **Easy debugging** - Query history to find issues
- **State management** - No manual tracking needed

**Key Principle:** Metadata drives the pipeline, not the other way around.

---

**Last Updated:** November 2024  
**Version:** 1.0