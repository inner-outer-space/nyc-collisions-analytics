{{
  config(
    materialized = 'incremental',
    unique_key = 'crash_id'
  )
}}

SELECT *
FROM
    {{ ref('stg_crash_inter') }} C
LEFT JOIN (
    SELECT
        weather_datetime,
        temp_c,
        --temp_f,
        precip_mm,
        weather_desc,
        lead(weather_datetime) over (order by weather_datetime) as next_weather_datetime
    FROM
        {{ ref('stg_weather_data') }}
) W 
ON C.crash_datetime >= W.weather_datetime
     and (C.crash_datetime < W.next_weather_datetime or W.next_weather_datetime is null)
