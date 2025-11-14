# Data Dictionary - NYC Taxi Pipeline

## **Overview**

This document provides comprehensive definitions for all columns across all tables in the NYC Taxi Data Pipeline.

---

## **Table of Contents**

- [Staging Layer](#staging-layer-bronze)
- [Raw Layer](#raw-layer-bronze)
- [Silver Layer](#silver-layer)
- [Gold Layer](#gold-layer)
- [Metadata Table](#metadata-table)

---

## **Staging Layer (Bronze)**

**Table:** `staging_yellow_taxi`  
**Purpose:** Stores raw data exactly as received from source  
**Partitioning:** By `source_month` (1-12)

### **Metadata Columns**

| Column Name | Data Type | Description | Example |
|-------------|-----------|-------------|---------|
| `source_month` | INT64 | Source parquet file month (1-12) | 1 (January) |
| `source_filename` | STRING | Original parquet filename | yellow_tripdata_2024-01.parquet |
| `upload_timestamp` | TIMESTAMP | When data was loaded to staging | 2024-11-13 10:30:00 UTC |

### **Trip Data Columns**

| Column Name | Data Type | Nullable | Description | Example | Valid Range |
|-------------|-----------|----------|-------------|---------|-------------|
| `VendorID` | INT64 | No | Taxi vendor identifier<br>1 = Creative Mobile Technologies<br>2 = VeriFone Inc. | 2 | 1, 2 |
| `tpep_pickup_datetime` | TIMESTAMP | No | Trip start date and time | 2024-01-15 08:30:00 | Any datetime |
| `tpep_dropoff_datetime` | TIMESTAMP | No | Trip end date and time | 2024-01-15 08:45:00 | >= pickup_datetime |
| `passenger_count` | FLOAT64 | Yes | Number of passengers | 2.0 | 0-9 (typical) |
| `trip_distance` | FLOAT64 | Yes | Trip distance in miles | 3.45 | >= 0 |
| `RatecodeID` | FLOAT64 | Yes | Rate code:<br>1 = Standard rate<br>2 = JFK<br>3 = Newark<br>4 = Nassau/Westchester<br>5 = Negotiated fare<br>6 = Group ride | 1.0 | 1-6 |
| `store_and_fwd_flag` | STRING | Yes | Trip record storage flag:<br>Y = Store and forward trip<br>N = Not a store and forward trip | N | Y, N |
| `PULocationID` | INT64 | Yes | Pickup taxi zone ID | 161 | 1-263 |
| `DOLocationID` | INT64 | Yes | Dropoff taxi zone ID | 234 | 1-263 |
| `payment_type` | INT64 | Yes | Payment method:<br>1 = Credit card<br>2 = Cash<br>3 = No charge<br>4 = Dispute<br>5 = Unknown<br>6 = Voided trip | 1 | 1-6 |
| `fare_amount` | FLOAT64 | Yes | Metered fare amount | 15.50 | >= -200 |
| `extra` | FLOAT64 | Yes | Extra charges (rush hour, overnight) | 2.50 | >= -10 |
| `mta_tax` | FLOAT64 | Yes | MTA tax (automatically triggered) | 0.50 | >= -1 |
| `tip_amount` | FLOAT64 | Yes | Tip amount (credit cards only) | 3.00 | >= 0 |
| `tolls_amount` | FLOAT64 | Yes | Total toll amount | 5.76 | >= 0 |
| `improvement_surcharge` | FLOAT64 | Yes | Improvement surcharge | 0.30 | >= -1 |
| `total_amount` | FLOAT64 | Yes | Total charge to passenger | 27.56 | >= -200 |
| `congestion_surcharge` | FLOAT64 | Yes | Congestion surcharge | 2.50 | >= -1 |
| `Airport_fee` | FLOAT64 | Yes | Airport fee | 1.25 | 0, 1.25 |

### **Data Quality Notes**

- Contains date infiltrations (trips from 2002, 2008, 2009, etc.)
- All data preserved as-is from source
- No filtering or validation applied
- Used for idempotency checking
- Partitioned for efficient queries

---

## **Raw Layer (Bronze)**

**Table:** `raw_yellow_taxi`  
**Purpose:** Validated 2024 data only, filtered from staging  
**Partitioning:** None (regular table)

### **Columns**

Same as Staging Layer **except**:
- No `source_month` (metadata removed)
- No `source_filename` (metadata removed)
- No `upload_timestamp` (metadata removed)
- Only 2024 trips (WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024)
- Removes date infiltrations

**Row Count (2024):** ~35-41 million rows

### **Filtering Logic**

```sql
SELECT 
    VendorID,
    tpep_pickup_datetime,
    -- ... all other columns except metadata
FROM staging_yellow_taxi
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024
```

---

## **Silver Layer (Silver)**

**Table:** `silver_yellow_taxi`  
**Purpose:** Cleaned and standardized data with business-friendly names  
**Partitioning:** None

### **Columns**

| Column Name | Data Type | Nullable | Description | Source Column | Transformation |
|-------------|-----------|----------|-------------|---------------|----------------|
| `vendor_id` | INT64 | No | Taxi vendor identifier | VendorID | Renamed |
| `pickup_datetime` | TIMESTAMP | No | Trip start datetime | tpep_pickup_datetime | Renamed |
| `dropoff_datetime` | TIMESTAMP | No | Trip end datetime | tpep_dropoff_datetime | Renamed |
| `passenger_count` | INT64 | Yes | Number of passengers | passenger_count | Cast to INT |
| `trip_distance` | FLOAT64 | Yes | Trip distance (miles) | trip_distance | No change |
| `rate_code_id` | INT64 | Yes | Rate code | RatecodeID | Cast to INT |
| `store_and_fwd_flag` | STRING | Yes | Storage flag | store_and_fwd_flag | No change |
| `pickup_location_id` | INT64 | Yes | Pickup zone | PULocationID | Renamed |
| `dropoff_location_id` | INT64 | Yes | Dropoff zone | DOLocationID | Renamed |
| `payment_type` | INT64 | Yes | Payment method | payment_type | No change |
| `fare_amount` | FLOAT64 | Yes | Base fare | fare_amount | No change |
| `extra` | FLOAT64 | Yes | Extra charges | extra | No change |
| `mta_tax` | FLOAT64 | Yes | MTA tax | mta_tax | No change |
| `tip_amount` | FLOAT64 | Yes | Tip amount | tip_amount | No change |
| `tolls_amount` | FLOAT64 | Yes | Tolls | tolls_amount | No change |
| `improvement_surcharge` | FLOAT64 | Yes | Improvement charge | improvement_surcharge | No change |
| `total_amount` | FLOAT64 | Yes | Total charge | total_amount | No change |
| `congestion_surcharge` | FLOAT64 | Yes | Congestion charge | congestion_surcharge | No change |
| `airport_fee` | FLOAT64 | Yes | Airport fee | Airport_fee | Renamed |

### **Data Quality Rules**

Applied during Silver layer creation:
- passenger_count cast to INT64
- rate_code_id cast to INT64
- Standardized column naming (snake_case)
- Descriptive names for business users

**Row Count:** ~30-35 million rows (after quality filters)

---

## **Gold Layer (Gold)**

**Table:** `gold_daily_metrics`  
**Purpose:** Aggregated business metrics for analytics  
**Granularity:** Daily + Payment Type

### **Columns**

| Column Name | Data Type | Nullable | Description | Calculation |
|-------------|-----------|----------|-------------|-------------|
| `trip_date` | DATE | No | Trip date (from pickup) | DATE(pickup_datetime) |
| `payment_type` | INT64 | No | Payment method | From silver layer |
| `trip_count` | INT64 | No | Total trips | COUNT(*) |
| `total_passengers` | INT64 | Yes | Total passengers | SUM(passenger_count) |
| `total_distance` | FLOAT64 | Yes | Total miles traveled | SUM(trip_distance) |
| `total_fare_amount` | FLOAT64 | Yes | Total fare revenue | SUM(fare_amount) |
| `total_tip_amount` | FLOAT64 | Yes | Total tips | SUM(tip_amount) |
| `total_amount` | FLOAT64 | Yes | Total revenue | SUM(total_amount) |
| `avg_fare_amount` | FLOAT64 | Yes | Average fare per trip | AVG(fare_amount) |
| `avg_trip_distance` | FLOAT64 | Yes | Average trip distance | AVG(trip_distance) |
| `avg_tip_amount` | FLOAT64 | Yes | Average tip per trip | AVG(tip_amount) |

### **Aggregation Logic**

```sql
SELECT
    DATE(pickup_datetime) as trip_date,
    payment_type,
    COUNT(*) as trip_count,
    SUM(passenger_count) as total_passengers,
    SUM(trip_distance) as total_distance,
    SUM(fare_amount) as total_fare_amount,
    SUM(tip_amount) as total_tip_amount,
    SUM(total_amount) as total_amount,
    AVG(fare_amount) as avg_fare_amount,
    AVG(trip_distance) as avg_trip_distance,
    AVG(tip_amount) as avg_tip_amount
FROM silver_yellow_taxi
GROUP BY trip_date, payment_type
ORDER BY trip_date, payment_type
```

**Row Count:** ~400-500 rows (365 days × payment types)

### **Business Insights**

This table enables analysis of:
- Daily trip volumes
- Revenue trends
- Payment method preferences
- Distance patterns
- Tipping behavior

---

## **Metadata Table**

**Table:** `pipeline_metadata`  
**Purpose:** Track all pipeline execution history

### **Columns**

| Column Name | Data Type | Nullable | Description | Example |
|-------------|-----------|----------|-------------|---------|
| `pipeline_name` | STRING | No | Pipeline type | incremental |
| `date_range` | STRING | No | Date range processed | 2024-01-01 to 2024-01-31 |
| `month_loaded` | STRING | No | Month name or "full year" | January |
| `status` | STRING | No | Execution status:<br>SUCCESS<br>FAILED<br>SKIPPED | SUCCESS |
| `rows_loaded` | INT64 | No | Number of rows processed | 2,964,609 |
| `run_timestamp` | TIMESTAMP | No | Execution timestamp | 2024-11-13 10:30:00 UTC |
| `runtime` | FLOAT64 | No | Execution time (seconds) | 1245.67 |
| `error_message` | STRING | Yes | Error details if failed | NULL |

### **Status Values**

| Status | Meaning | Action Taken |
|--------|---------|--------------|
| `SUCCESS` | Pipeline completed successfully | Rows loaded, tables updated |
| `FAILED` | Pipeline encountered error | Logged, can retry |
| `SKIPPED` | Data already exists | No action, progresses to next |

### **Usage**

This table is used to:
- Track pipeline execution history
- Determine next month to load
- Monitor pipeline health
- Debug failures
- Calculate performance metrics

---

## **Data Type Conventions**

### **Naming Standards**

- **Staging/Raw:** Original column names preserved
- **Silver:** snake_case, descriptive names
- **Gold:** Business-friendly aggregation names

### **Type Conversions**

| Raw Type | Silver Type | Reason |
|----------|-------------|--------|
| FLOAT64 (passenger_count) | INT64 | Always whole numbers |
| FLOAT64 (RatecodeID) | INT64 | Always whole numbers |
| tpep_* | pickup/dropoff_* | More descriptive |

---

## **Common Queries**

### **Check Staging Partitions**

```sql
SELECT 
    source_month,
    COUNT(*) as row_count,
    MIN(tpep_pickup_datetime) as min_date,
    MAX(tpep_pickup_datetime) as max_date
FROM `staging_yellow_taxi`
GROUP BY source_month
ORDER BY source_month;
```

### **Verify 2024 Data in Raw**

```sql
SELECT 
    EXTRACT(YEAR FROM tpep_pickup_datetime) as year,
    COUNT(*) as row_count
FROM `raw_yellow_taxi`
GROUP BY year
ORDER BY year;
```

### **Silver Layer Stats**

```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(DISTINCT DATE(pickup_datetime)) as unique_days,
    MIN(pickup_datetime) as first_trip,
    MAX(pickup_datetime) as last_trip
FROM `silver_yellow_taxi`;
```

### **Gold Layer Summary**

```sql
SELECT 
    payment_type,
    COUNT(*) as days_with_data,
    SUM(trip_count) as total_trips,
    SUM(total_amount) as total_revenue
FROM `gold_daily_metrics`
GROUP BY payment_type
ORDER BY payment_type;
```

---

## **Reference Documentation**

- **NYC TLC Data Dictionary:** [Official Documentation](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- **Taxi Zone Lookup:** [Zone Mappings](https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv)

---

## **Notes**

- All datetime columns are in UTC
- Monetary amounts in USD
- Distance in miles
- Negative values may indicate refunds/corrections
- NULL values indicate missing/not applicable data

---

**Last Updated:** November 2024  
**Version:** 1.0