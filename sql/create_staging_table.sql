-- Create Staging Table for NYC Taxi Data
-- This table stores raw parquet data downloaded from source
-- Permanent table - data persists across pipeline runs

CREATE TABLE IF NOT EXISTS `nyc-taxi-pipeline-477912.nyc_taxi_dataset.staging_yellow_taxi` (
    VendorID INT64,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT64,
    trip_distance FLOAT64,
    RatecodeID INT64,
    store_and_fwd_flag STRING,
    PULocationID INT64,
    DOLocationID INT64,
    payment_type INT64,
    fare_amount FLOAT64,
    extra FLOAT64,
    mta_tax FLOAT64,
    tip_amount FLOAT64,
    tolls_amount FLOAT64,
    improvement_surcharge FLOAT64,
    total_amount FLOAT64,
    congestion_surcharge FLOAT64,
    Airport_fee FLOAT64
)
OPTIONS(
    description="Staging table for NYC Yellow Taxi trip data - permanent storage"
);