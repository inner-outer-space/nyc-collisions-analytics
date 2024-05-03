{{ config(materialized='view') }}
WITH formatted_data AS (
  SELECT
        -- UNIQUE KEY
        cast(collision_id as integer) as crash_id,

        -- TIME FIELDS (local timezone - NYC, the UTC timezone is incorrect and needs to be dropped)
        format_timestamp('%Y-%m-%d %H:%M', crash_timestamp) AS crash_timestamp_string,  
        sun_phase,

        -- LOCATION FIELDS 
        COALESCE(borough, 'Unknown') as borough,
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
        {{ source('staging','crash_data_external') }}
)

SELECT
    -- UNIQUE KEY
    crash_id,

    -- TIME_FIELDS (local timezone - NYC)
    parse_datetime('%Y-%m-%d %H:%M', crash_timestamp_string) as crash_datetime,


    -- THE REST OF THE COLUMNS
    formatted_data.* EXCEPT(crash_id, crash_timestamp_string )
    
FROM 
    formatted_data

-- dbt build --m <model.sql> --vars 'is_test_run: false'
--{% if var('is_test_run', default=true) %}

--  limit 100

--{% endif %}