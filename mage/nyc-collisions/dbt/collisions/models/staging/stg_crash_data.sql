{{ config(materialized='view') }}

SELECT
    -- UNIQUE KEY
    collision_id as crash_id,

    -- TIME_FIELDS (local timezone - NYC)
    TIMESTAMP(crash_datetime) as crash_datetime,
    extract(date from crash_datetime) as crash_date,
    extract(time from crash_datetime) as crash_time,
    extract(month from crash_datetime) as crash_month, 
    extract(year from crash_datetime) as crash_year, 
    extract(dayofweek from crash_datetime) as crash_dow,
    format_timestamp('%A', crash_datetime) as crash_dow_name,
    format_timestamp('%B %Y', crash_datetime) as crash_mon_yr,
    sun_phase, 


    -- LOCATION FIELDS 
    borough,
    zip_code,
    latitude,
    longitude,
    on_street_name,
    off_street_name,
    cross_street_name,

    -- PEOPLE DETAILS
    number_of_persons_injured,
    number_of_persons_killed,
    number_of_pedestrians_injured,
    number_of_pedestrians_killed,
    number_of_cyclist_injured,
    number_of_cyclist_killed,
    number_of_motorist_injured,
    number_of_motorist_killed,
    
    -- REASON DETAILS 
    contributing_factor_vehicle_1,
    contributing_factor_vehicle_2,
    contributing_factor_vehicle_3,
    contributing_factor_vehicle_4,
    contributing_factor_vehicle_5,

    -- CARS INVOLVED
    vehicle_type_code1 as vehicle_type_code_1,
    vehicle_type_code2 as vehicle_type_code_2,
    vehicle_type_code_3,
    vehicle_type_code_4,
    vehicle_type_code_5

FROM 
    {{ source('staging','crash_external_data') }}
LIMIT 10