WITH vehicle_join AS (
    SELECT
        vehicle,
        contributing_factor
    FROM
        {{ ref('fact_crash_weather_enriched') }},
        UNNEST([vehicle_type_code_1, vehicle_type_code_2, vehicle_type_code_3, vehicle_type_code_4, vehicle_type_code_5]) AS vehicle
        WITH OFFSET AS vehicle_offset,
        UNNEST([contributing_factor_vehicle_1, contributing_factor_vehicle_2, contributing_factor_vehicle_3, contributing_factor_vehicle_4, contributing_factor_vehicle_5]) AS contributing_factor
        WITH OFFSET AS factor_offset
    WHERE
        vehicle_offset = factor_offset
)
SELECT
    vehicle, 
    contributing_factor,
    COUNT(*) AS collision_count,
FROM 
    vehicle_join
GROUP BY 
    vehicle,
    contributing_factor
ORDER BY 
    vehicle, 
    collision_count DESC