-- Create Gold Layer Table
-- Contains aggregated analytics-ready data
-- Dropped and recreated on every pipeline run

CREATE OR REPLACE TABLE `nyc-taxi-pipeline-477912.nyc_taxi_dataset.gold_yellow_taxi` AS

WITH monthly_stats AS (
    SELECT
        pickup_year,
        pickup_month,
        FORMAT_DATE('%B', DATE(pickup_year, pickup_month, 1)) AS month_name,
        DATE(pickup_year, pickup_month, 1) AS month_start_date,
        
        -- Trip metrics
        COUNT(*) AS total_trips,
        SUM(trip_distance) AS total_distance,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration_minutes,
        ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph,
        
        -- Passenger metrics
        SUM(passenger_count) AS total_passengers,
        ROUND(AVG(passenger_count), 2) AS avg_passengers_per_trip,
        
        -- Revenue metrics
        ROUND(SUM(fare_amount), 2) AS total_fare,
        ROUND(AVG(fare_amount), 2) AS avg_fare,
        ROUND(SUM(tip_amount), 2) AS total_tips,
        ROUND(AVG(tip_amount), 2) AS avg_tip,
        ROUND(SUM(total_amount), 2) AS total_revenue,
        ROUND(AVG(total_amount), 2) AS avg_total,
        
        -- Payment type breakdown
        SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
        SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
        ROUND(SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS credit_card_pct,
        
        -- Time-based metrics
        SUM(CASE WHEN pickup_hour BETWEEN 6 AND 9 THEN 1 ELSE 0 END) AS morning_rush_trips,
        SUM(CASE WHEN pickup_hour BETWEEN 16 AND 19 THEN 1 ELSE 0 END) AS evening_rush_trips,
        SUM(CASE WHEN pickup_dayofweek IN (1, 7) THEN 1 ELSE 0 END) AS weekend_trips,
        SUM(CASE WHEN pickup_dayofweek BETWEEN 2 AND 6 THEN 1 ELSE 0 END) AS weekday_trips
        
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY pickup_year, pickup_month
),

daily_stats AS (
    SELECT
        DATE(pickup_datetime) AS trip_date,
        FORMAT_DATE('%A', DATE(pickup_datetime)) AS day_name,
        pickup_dayofweek,
        
        COUNT(*) AS daily_trips,
        ROUND(SUM(total_amount), 2) AS daily_revenue,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration,
        ROUND(AVG(fare_amount), 2) AS avg_fare
        
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY trip_date, day_name, pickup_dayofweek
),

hourly_stats AS (
    SELECT
        pickup_hour,
        CONCAT('Hour ', CAST(pickup_hour AS STRING), ':00') AS hour_label,
        
        COUNT(*) AS trips_per_hour,
        ROUND(AVG(trip_distance), 2) AS avg_distance,
        ROUND(AVG(total_amount), 2) AS avg_revenue,
        ROUND(AVG(trip_duration_minutes), 2) AS avg_duration,
        ROUND(SUM(total_amount), 2) AS total_revenue_hour
        
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY pickup_hour
),

location_stats AS (
    SELECT
        PULocationID AS location_id,
        COUNT(*) AS pickup_count,
        ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
        ROUND(AVG(total_amount), 2) AS avg_revenue,
        ROUND(SUM(total_amount), 2) AS total_revenue_location
        
    FROM `nyc-taxi-pipeline-477912.nyc_taxi_dataset.silver_yellow_taxi`
    GROUP BY PULocationID
    HAVING COUNT(*) > 1000  -- Only significant locations
    ORDER BY pickup_count DESC
    LIMIT 100
)

-- Combine all aggregations
SELECT
    'monthly' AS aggregation_type,
    CAST(pickup_month AS STRING) AS dimension_value,
    month_name AS dimension_label,
    month_start_date AS reference_date,
    total_trips,
    total_distance,
    avg_distance,
    avg_duration_minutes,
    avg_speed_mph,
    total_revenue,
    avg_total AS avg_revenue_per_trip,
    total_passengers,
    avg_passengers_per_trip,
    credit_card_trips,
    cash_trips,
    credit_card_pct,
    morning_rush_trips,
    evening_rush_trips,
    weekend_trips,
    weekday_trips,
    NULL AS pickup_count,
    NULL AS hour_label
FROM monthly_stats

UNION ALL

SELECT
    'daily' AS aggregation_type,
    CAST(trip_date AS STRING) AS dimension_value,
    day_name AS dimension_label,
    trip_date AS reference_date,
    daily_trips AS total_trips,
    NULL AS total_distance,
    avg_distance,
    avg_duration AS avg_duration_minutes,
    NULL AS avg_speed_mph,
    daily_revenue AS total_revenue,
    ROUND(daily_revenue / daily_trips, 2) AS avg_revenue_per_trip,
    NULL AS total_passengers,
    NULL AS avg_passengers_per_trip,
    NULL AS credit_card_trips,
    NULL AS cash_trips,
    NULL AS credit_card_pct,
    NULL AS morning_rush_trips,
    NULL AS evening_rush_trips,
    CASE WHEN pickup_dayofweek IN (1, 7) THEN daily_trips ELSE 0 END AS weekend_trips,
    CASE WHEN pickup_dayofweek BETWEEN 2 AND 6 THEN daily_trips ELSE 0 END AS weekday_trips,
    NULL AS pickup_count,
    NULL AS hour_label
FROM daily_stats

UNION ALL

SELECT
    'hourly' AS aggregation_type,
    CAST(pickup_hour AS STRING) AS dimension_value,
    hour_label AS dimension_label,
    NULL AS reference_date,
    trips_per_hour AS total_trips,
    NULL AS total_distance,
    avg_distance,
    avg_duration AS avg_duration_minutes,
    NULL AS avg_speed_mph,
    total_revenue_hour AS total_revenue,
    avg_revenue AS avg_revenue_per_trip,
    NULL AS total_passengers,
    NULL AS avg_passengers_per_trip,
    NULL AS credit_card_trips,
    NULL AS cash_trips,
    NULL AS credit_card_pct,
    NULL AS morning_rush_trips,
    NULL AS evening_rush_trips,
    NULL AS weekend_trips,
    NULL AS weekday_trips,
    NULL AS pickup_count,
    hour_label
FROM hourly_stats

UNION ALL

SELECT
    'top_locations' AS aggregation_type,
    CAST(location_id AS STRING) AS dimension_value,
    CONCAT('Location ', CAST(location_id AS STRING)) AS dimension_label,
    NULL AS reference_date,
    NULL AS total_trips,
    NULL AS total_distance,
    avg_trip_distance AS avg_distance,
    NULL AS avg_duration_minutes,
    NULL AS avg_speed_mph,
    total_revenue_location AS total_revenue,
    avg_revenue AS avg_revenue_per_trip,
    NULL AS total_passengers,
    NULL AS avg_passengers_per_trip,
    NULL AS credit_card_trips,
    NULL AS cash_trips,
    NULL AS credit_card_pct,
    NULL AS morning_rush_trips,
    NULL AS evening_rush_trips,
    NULL AS weekend_trips,
    NULL AS weekday_trips,
    pickup_count,
    NULL AS hour_label
FROM location_stats

ORDER BY aggregation_type, reference_date, dimension_value;