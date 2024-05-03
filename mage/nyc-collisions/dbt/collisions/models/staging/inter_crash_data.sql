{{ config(materialized='view') }}

SELECT
    C.*,

    -- Add calculated time values here instead of in looker
    extract(date from crash_datetime) as crash_date,
    extract(time from crash_datetime) as crash_time,
    extract(month from crash_datetime) as crash_month, 
    extract(year from crash_datetime) as crash_year, 
    extract(dayofweek from crash_datetime) as crash_dow,
    format_timestamp('%A', crash_datetime) as crash_dow_name,
    format_timestamp('%Y-%m', crash_datetime) as crash_yr_mon,

    -- Add number of vehicles involved in collision
    (CASE WHEN vehicle_type_code_1 IS NOT NULL AND vehicle_type_code_1 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_2 IS NOT NULL AND vehicle_type_code_2 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_3 IS NOT NULL AND vehicle_type_code_3 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_4 IS NOT NULL AND vehicle_type_code_4 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_5 IS NOT NULL AND vehicle_type_code_5 != '' THEN 1 ELSE 0 END) AS vehicles_count,

    -- Add severity
    {{ calculate_severity('C.') }} as severity,

    -- Add total counts of inuries and fatalities
    COALESCE(number_of_pedestrians_injured, 0) 
    + COALESCE(number_of_cyclist_injured, 0) 
    + COALESCE(number_of_motorist_injured, 0) AS total_injuries,
    COALESCE(number_of_pedestrians_killed, 0) 
    + COALESCE(number_of_cyclist_killed, 0) 
    + COALESCE(number_of_motorist_killed, 0) AS total_fatalities
FROM
    {{ ref('stg_crash_data') }} as C
    --{{ ref('stg_crash_data') }} as C