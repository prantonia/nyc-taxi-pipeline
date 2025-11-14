-- Create Silver Layer Table
-- Contains cleaned and transformed data
-- Dropped and recreated on every pipeline run

CREATE OR REPLACE TABLE `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi` AS
SELECT
    -- Keep original identifiers
    VendorID,
    tpep_pickup_datetime AS pickup_datetime,      -- Renamed for clarity
    tpep_dropoff_datetime AS dropoff_datetime,    -- Renamed for clarity
    
    -- Calculate trip duration in minutes
    TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) AS trip_duration_minutes,
    
    -- Clean passenger count - remove nulls and negatives
    CASE 
        WHEN passenger_count IS NULL OR passenger_count <= 0 THEN 1
        WHEN passenger_count > 6 THEN 6  -- Cap at reasonable max
        ELSE CAST(passenger_count AS INT64)
    END AS passenger_count,
    
    -- Clean trip distance - remove nulls and negatives
    CASE 
        WHEN trip_distance IS NULL OR trip_distance <= 0 THEN 0
        ELSE trip_distance
    END AS trip_distance,
    
    -- Standardize RatecodeID
    CAST(COALESCE(RatecodeID, 1) AS INT64) AS RatecodeID,
    
    -- Standardize store and forward flag
    UPPER(COALESCE(store_and_fwd_flag, 'N')) AS store_and_fwd_flag,
    
    -- Location IDs
    PULocationID,
    DOLocationID,
    
    -- Standardize payment type
    CAST(COALESCE(payment_type, 1) AS INT64) AS payment_type,
    
    -- Clean monetary amounts - remove negatives
    CASE WHEN fare_amount < 0 THEN 0 ELSE COALESCE(fare_amount, 0) END AS fare_amount,
    CASE WHEN extra < 0 THEN 0 ELSE COALESCE(extra, 0) END AS extra,
    CASE WHEN mta_tax < 0 THEN 0 ELSE COALESCE(mta_tax, 0) END AS mta_tax,
    CASE WHEN tip_amount < 0 THEN 0 ELSE COALESCE(tip_amount, 0) END AS tip_amount,
    CASE WHEN tolls_amount < 0 THEN 0 ELSE COALESCE(tolls_amount, 0) END AS tolls_amount,
    CASE WHEN improvement_surcharge < 0 THEN 0 ELSE COALESCE(improvement_surcharge, 0) END AS improvement_surcharge,
    CASE WHEN total_amount < 0 THEN 0 ELSE COALESCE(total_amount, 0) END AS total_amount,
    CASE WHEN congestion_surcharge < 0 THEN 0 ELSE COALESCE(congestion_surcharge, 0) END AS congestion_surcharge,
    CASE WHEN Airport_fee < 0 THEN 0 ELSE COALESCE(Airport_fee, 0) END AS Airport_fee,
    
    -- Extract temporal features
    EXTRACT(YEAR FROM tpep_pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM tpep_pickup_datetime) AS pickup_month,
    EXTRACT(DAY FROM tpep_pickup_datetime) AS pickup_day,
    EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
    EXTRACT(DAYOFWEEK FROM tpep_pickup_datetime) AS pickup_dayofweek,
    FORMAT_DATE('%A', DATE(tpep_pickup_datetime)) AS pickup_day_name,
    
    -- Calculate speed (mph) - only for valid trips
    CASE 
        WHEN trip_distance > 0 
            AND TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE) > 0
        THEN (trip_distance / TIMESTAMP_DIFF(tpep_dropoff_datetime, tpep_pickup_datetime, MINUTE)) * 60
        ELSE 0
    END AS avg_speed_mph

FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.raw_yellow_taxi`

WHERE 
    -- Filter out invalid records
    tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_pickup_datetime < tpep_dropoff_datetime  -- Valid time range
    AND tpep_pickup_datetime >= '2024-01-01'
    AND tpep_pickup_datetime < '2025-01-01'
    AND VendorID IS NOT NULL
    AND passenger_count > 0
    AND trip_distance > 0
    AND fare_amount >= 0
    AND total_amount >= 0

-- Sort output by pickup time (ascending)
ORDER BY pickup_datetime ASC;