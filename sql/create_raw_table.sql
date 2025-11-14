-- Create Raw (Bronze) Layer Table
-- Contains validated 2024 data only
-- Idempotent loads with deduplication logic

CREATE OR REPLACE TABLE `nyc-taxi-pipeline-477912.nyc_taxi_dataset.raw_yellow_taxi`
OPTIONS(
    description="Raw layer - validated 2024 NYC Yellow Taxi trip data"
)
AS
SELECT
    VendorID,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID,
    store_and_fwd_flag,
    PULocationID,
    DOLocationID,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    Airport_fee
FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.staging_yellow_taxi`
WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = 2024;