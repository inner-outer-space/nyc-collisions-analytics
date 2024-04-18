{{ config(materialized='view') }}

WITH formatted_weather_data AS (
  SELECT
        -- TIME FIELDS (local timezone - NYC)
        format_date('%Y-%m-%d', parse_date('%Y-%m-%d', date)) as weather_date,
        format_time('%H:%M',parse_time('%H%M', format('%04d', cast(time as int)))) as weather_time,

        -- WEATHER METRICS
        temp_c,
        temp_f,
        windspeed_miles,
        windspeed_kmph,
        winddir_degree,
        winddir16point,
        weather_code,
        weather_desc,
        precip_mm
    FROM
        {{ source('staging','weather_data_external') }}
)

SELECT 
    -- TIME FIELDS (local timezone - NYC)
    parse_datetime('%Y-%m-%d %H:%M', concat(weather_date, ' ', weather_time)) as weather_datetime,
    weather_date,
    weather_time,

    -- WEATHER METRICS
    temp_c,
    temp_f,
    windspeed_miles,
    windspeed_kmph,
    winddir_degree,
    winddir16point,
    weather_code,
    weather_desc,
    precip_mm
FROM 
    formatted_weather_data

-- dbt build --m <model.sql> --vars 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}