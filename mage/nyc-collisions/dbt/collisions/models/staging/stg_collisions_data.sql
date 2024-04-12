{{ config(materialized='view') }}

WITH formatted_data AS (
  SELECT
        -- KEY
        cast(collision_id as integer) as collision_id,

        -- TIME FIELDS (local timezone - NYC)
        date(crash_date) as formatted_crash_date,
        format_time('%H:%M', parse_time('%H:%M', crash_time)) as formatted_crash_time,
        parse_datetime('%Y-%m-%d %H:%M', concat(date(crash_date),' ', format_time('%H:%M', parse_time('%H:%M', crash_time)))) as crash_datetime,

        -- LOCATION FIELDS 
        borough,
        cast(zip_code as integer) as zip_code,
        cast(latitude as numeric) as latitude,
        cast(longitude as numeric) as longitude,
        on_street_name,
        off_street_name,
        cross_street_name,

        -- PEOPLE DETAILS
        cast(number_of_persons_injured as integer) as number_of_persons_injured,
        cast(number_of_persons_killed as integer) as number_of_persons_killed,
        cast(number_of_pedestrians_injured as integer) as number_of_pedestrians_injured,
        cast(number_of_pedestrians_killed as integer) as number_of_pedestrians_killed,
        cast(number_of_cyclist_injured as integer) as number_of_cyclist_injured,
        cast(number_of_cyclist_killed as integer) as number_of_cyclist_killed,
        cast(number_of_motorist_injured as integer) as number_of_motorist_injured,
        cast(number_of_motorist_killed as integer) as number_of_motorist_killed,
        
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
        {{ source('staging','external_data') }}
)

SELECT
    -- KEY
    cast(collision_id as integer) as collision_id,

    -- TIME_FIELDS (local timezone - NYC)
    crash_datetime,
    formatted_crash_date as crash_date,
    formatted_crash_time as crash_time,
    crash_datetime,
    extract(month from crash_datetime) as crash_month, 
    extract(year from crash_datetime) as crash_year, 
    extract(dayofweek from crash_datetime) as crash_dow,
    format_timestamp('%B %Y', crash_datetime) as crash_mon_yr,
    format_timestamp('%A', crash_datetime) as crash_dow_name,

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
    vehicle_type_code_1,
    vehicle_type_code_2,
    vehicle_type_code_3,
    vehicle_type_code_4,
    vehicle_type_code_5


FROM 
    formatted_data

LIMIT 1000