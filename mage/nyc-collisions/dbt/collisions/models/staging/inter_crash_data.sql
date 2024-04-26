{{ config(materialized='view') }}

SELECT
    C.*,
    {{ calculate_severity('C.') }} as severity,
    COALESCE(number_of_pedestrians_injured, 0) 
    + COALESCE(number_of_cyclist_injured, 0) 
    + COALESCE(number_of_motorist_injured, 0) AS total_persons_injured,
    COALESCE(number_of_pedestrians_killed, 0) 
    + COALESCE(number_of_cyclist_killed, 0) 
    + COALESCE(number_of_motorist_killed, 0) AS total_persons_killed
FROM
    {{ ref('stg_crash_data') }} as C