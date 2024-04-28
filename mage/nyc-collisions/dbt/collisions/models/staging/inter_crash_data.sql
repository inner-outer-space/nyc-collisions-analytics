{{ config(materialized='view') }}

SELECT
    C.*,
    (CASE WHEN vehicle_type_code_1 IS NOT NULL AND vehicle_type_code_1 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_2 IS NOT NULL AND vehicle_type_code_2 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_3 IS NOT NULL AND vehicle_type_code_3 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_4 IS NOT NULL AND vehicle_type_code_4 != '' THEN 1 ELSE 0 END +
    CASE WHEN vehicle_type_code_5 IS NOT NULL AND vehicle_type_code_5 != '' THEN 1 ELSE 0 END) AS vehicles_count,
    {{ calculate_severity('C.') }} as severity,
    COALESCE(number_of_pedestrians_injured, 0) 
    + COALESCE(number_of_cyclist_injured, 0) 
    + COALESCE(number_of_motorist_injured, 0) AS total_persons_injured,
    COALESCE(number_of_pedestrians_killed, 0) 
    + COALESCE(number_of_cyclist_killed, 0) 
    + COALESCE(number_of_motorist_killed, 0) AS total_persons_killed
FROM
    {{ ref('stg_crash_data') }} as C