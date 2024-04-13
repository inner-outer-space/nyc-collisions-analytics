{{ config(materialized='table') }}

SELECT
    C.*,
    {{ calculate_severity('C.')}} as severity,
    W.weather_datetime,
    W.temp_c,
    W.temp_f,
    W.precip_mm,
    W.weather_desc
FROM
    {{ ref('stg_crash_data') }} C
LEFT JOIN (
    SELECT
        weather_datetime,
        temp_c,
        temp_f,
        precip_mm,
        weather_desc,
        lead(weather_datetime) over (order by weather_datetime) as next_weather_datetime
    FROM
        {{ ref('stg_weather_data') }}
) W 
ON C.crash_datetime >= W.weather_datetime
     and (C.crash_datetime < W.next_weather_datetime or W.next_weather_datetime is null)
