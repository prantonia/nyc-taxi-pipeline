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
| `pickup_year` | INT64 | No | Year of pickup | tpep_pickup_datetime | EXTRACT(YEAR) |
| `pickup_month` | INT64 | No | Month of pickup (1-12) | tpep_pickup_datetime | EXTRACT(MONTH) |
| `pickup_hour` | INT64 | No | Hour of pickup (0-23) | tpep_pickup_datetime | EXTRACT(HOUR) |
| `pickup_dayofweek` | INT64 | No | Day of week (1=Sun, 7=Sat) | tpep_pickup_datetime | EXTRACT(DAYOFWEEK) |
| `passenger_count` | INT64 | Yes | Number of passengers | passenger_count | Cast to INT |
| `trip_distance` | FLOAT64 | Yes | Trip distance (miles) | trip_distance | No change |
| `trip_duration_minutes` | FLOAT64 | Yes | Trip duration in minutes | Calculated | TIMESTAMP_DIFF/60 |
| `avg_speed_mph` | FLOAT64 | Yes | Average speed (mph) | Calculated | distance/duration |
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
- Time-based columns extracted for analysis
- Trip duration and speed calculated
- Standardized column naming (snake_case)
- Descriptive names for business users

**Row Count:** ~30-35 million rows (after quality filters)

---

## **Gold Layer (Gold)**

**Table:** `gold_yellow_taxi`

**Purpose:** Pre-aggregated, analytics-ready data organized by aggregation type

**Granularity:** Multiple (monthly, daily, hourly, top locations)

**Refresh Strategy:** Full table replacement (CREATE OR REPLACE)

### **Overview**

The Gold layer contains multiple aggregation types in a single unified table. Each row represents statistics for a specific aggregation level (monthly, daily, hourly, or location-based). This design enables flexible analytics while maintaining a simple schema.

### **Columns**

| Column Name | Data Type | Nullable | Description | Populated For |
|-------------|-----------|----------|-------------|---------------|
| `aggregation_type` | STRING | No | Type of aggregation | All rows |
| `dimension_value` | STRING | No | Primary dimension identifier | All rows |
| `dimension_label` | STRING | No | Human-readable label | All rows |
| `reference_date` | DATE | Yes | Reference date for time-based aggregations | monthly, daily |
| `total_trips` | INT64 | Yes | Total number of trips | monthly, daily, hourly |
| `total_distance` | FLOAT64 | Yes | Sum of trip distances (miles) | monthly |
| `avg_distance` | FLOAT64 | Yes | Average trip distance (miles) | All |
| `avg_duration_minutes` | FLOAT64 | Yes | Average trip duration | monthly, daily, hourly |
| `avg_speed_mph` | FLOAT64 | Yes | Average speed (mph) | monthly |
| `total_revenue` | FLOAT64 | Yes | Total revenue collected | All |
| `avg_revenue_per_trip` | FLOAT64 | Yes | Average revenue per trip | All |
| `total_passengers` | INT64 | Yes | Total passengers transported | monthly |
| `avg_passengers_per_trip` | FLOAT64 | Yes | Average passengers per trip | monthly |
| `credit_card_trips` | INT64 | Yes | Number of credit card payments | monthly |
| `cash_trips` | INT64 | Yes | Number of cash payments | monthly |
| `credit_card_pct` | FLOAT64 | Yes | Percentage paid by credit card | monthly |
| `morning_rush_trips` | INT64 | Yes | Trips 6 AM - 9 AM | monthly |
| `evening_rush_trips` | INT64 | Yes | Trips 4 PM - 7 PM | monthly |
| `weekend_trips` | INT64 | Yes | Trips on Sat/Sun | monthly, daily |
| `weekday_trips` | INT64 | Yes | Trips Mon-Fri | monthly, daily |
| `pickup_count` | INT64 | Yes | Pickup count at location | top_locations |
| `hour_label` | STRING | Yes | Formatted hour label | hourly |

---

## **Aggregation Types**

### **1. Monthly Aggregation** (`aggregation_type = 'monthly'`)

**Purpose:** Month-level statistics for trend analysis  
**Granularity:** One row per month (12 rows for full year)

**Key Fields:**
- `dimension_value`: Month number ('1', '2', ..., '12')
- `dimension_label`: Month name ('January', 'February', etc.)
- `reference_date`: First day of month (2024-01-01, 2024-02-01, etc.)

**Populated Metrics:**
- All trip metrics (trips, distance, duration, speed)
- All revenue metrics
- All passenger metrics
- Payment type breakdown
- Rush hour analysis
- Weekend vs weekday split

**Example Row:**
```
aggregation_type: 'monthly'
dimension_value: '1'
dimension_label: 'January'
reference_date: 2024-01-01
total_trips: 2964624
total_revenue: 54123456.78
avg_distance: 3.25
credit_card_pct: 76.0
```

---

### **2. Daily Aggregation** (`aggregation_type = 'daily'`)

**Purpose:** Daily pattern analysis  
**Granularity:** One row per day (365 rows for full year)

**Key Fields:**
- `dimension_value`: Date string ('2024-01-15')
- `dimension_label`: Day name ('Monday', 'Tuesday', etc.)
- `reference_date`: The trip date

**Populated Metrics:**
- total_trips, avg_distance, avg_duration_minutes
- total_revenue, avg_revenue_per_trip
- weekend_trips, weekday_trips

**Example Row:**
```
aggregation_type: 'daily'
dimension_value: '2024-01-15'
dimension_label: 'Monday'
reference_date: 2024-01-15
total_trips: 95000
total_revenue: 1750000.50
avg_distance: 3.15
```

---

### **3. Hourly Aggregation** (`aggregation_type = 'hourly'`)

**Purpose:** Hour-of-day pattern analysis  
**Granularity:** One row per hour (24 rows)

**Key Fields:**
- `dimension_value`: Hour number ('0', '1', ..., '23')
- `hour_label`: Formatted hour ('Hour 0:00', 'Hour 14:00', etc.)

**Populated Metrics:**
- total_trips (as trips_per_hour)
- avg_distance, avg_duration_minutes
- total_revenue, avg_revenue_per_trip

**Example Row:**
```
aggregation_type: 'hourly'
dimension_value: '14'
hour_label: 'Hour 14:00'
total_trips: 125000
total_revenue: 2250000.00
avg_distance: 3.5
```

---

### **4. Top Locations Aggregation** (`aggregation_type = 'top_locations'`)

**Purpose:** Busiest pickup location analysis  
**Granularity:** Top 100 locations (with > 1000 pickups)

**Key Fields:**
- `dimension_value`: Location ID string ('237', '161', etc.)
- `dimension_label`: Location label ('Location 237', etc.)

**Populated Metrics:**
- pickup_count
- avg_distance (as avg_trip_distance)
- total_revenue, avg_revenue_per_trip

**Filter Criteria:**
- Minimum 1000 pickups
- Top 100 by pickup count

**Example Row:**
```
aggregation_type: 'top_locations'
dimension_value: '237'
dimension_label: 'Location 237'
pickup_count: 45000
total_revenue: 825000.00
avg_distance: 2.8
```

---

## **Gold Layer Usage Examples**

### **Query Monthly Trends**
```sql
SELECT 
    dimension_label AS month,
    total_trips,
    total_revenue,
    avg_distance,
    credit_card_pct
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

### **Query Peak Hours**
```sql
SELECT 
    hour_label,
    total_trips,
    avg_revenue_per_trip
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'hourly'
ORDER BY total_trips DESC
LIMIT 5;
```

### **Query Top Pickup Locations**
```sql
SELECT 
    dimension_label AS location,
    pickup_count,
    total_revenue,
    avg_distance
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'top_locations'
ORDER BY pickup_count DESC
LIMIT 10;
```

### **Compare Weekend vs Weekday**
```sql
SELECT 
    dimension_label AS month,
    weekday_trips,
    weekend_trips,
    ROUND(weekend_trips * 100.0 / (weekday_trips + weekend_trips), 2) AS weekend_pct
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi`
WHERE aggregation_type = 'monthly'
ORDER BY reference_date;
```

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
- Determine next month to load (incremental mode)
- Monitor pipeline health and performance
- Debug failures with detailed error messages
- Calculate runtime metrics and trends

---

## **Data Type Conventions**

### **Naming Standards**

- **Staging/Raw:** Original column names preserved
- **Silver:** snake_case, descriptive names with calculated fields
- **Gold:** Business-friendly aggregation names

### **Type Conversions**

| Raw Type | Silver Type | Reason |
|----------|-------------|--------|
| FLOAT64 (passenger_count) | INT64 | Always whole numbers |
| FLOAT64 (RatecodeID) | INT64 | Always whole numbers |
| tpep_* | pickup/dropoff_* | More descriptive |
| - | pickup_year, pickup_month, etc. | Extracted for analysis |
| - | trip_duration_minutes | Calculated field |
| - | avg_speed_mph | Calculated field |

---

## **Business Metric Definitions**

### **Revenue Metrics**
- `total_revenue` = Sum of all total_amount (fare + tips + surcharges + taxes)
- `avg_revenue_per_trip` = total_revenue / total_trips
- `credit_card_pct` = (credit_card_trips / total_trips) × 100

### **Operational Metrics**
- `avg_speed_mph` = trip_distance / (trip_duration_minutes / 60)
- `trip_duration_minutes` = TIMESTAMP_DIFF(dropoff, pickup) / 60
- `morning_rush_trips` = Trips between 6:00 AM - 9:00 AM
- `evening_rush_trips` = Trips between 4:00 PM - 7:00 PM

### **Time Classifications**
- `weekday` = Monday-Friday (DAYOFWEEK 2-6)
- `weekend` = Saturday-Sunday (DAYOFWEEK 1, 7)
- Payment Type: 1=Credit Card, 2=Cash

---

## **Data Quality Notes**

### **Gold Layer**
- All monetary values rounded to 2 decimal places
- All averages rounded to 2 decimal places
- Top locations filtered to > 1000 pickups (significance threshold)
- Limited to top 100 locations by volume
- Full table refresh on every pipeline run (no incremental update)

### **Silver Layer**
- Negative durations filtered out
- Zero distance trips handled
- NULL values preserved for optional fields
- Calculated fields may be NULL if source data incomplete

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
- Gold layer uses unified schema for all aggregation types

---