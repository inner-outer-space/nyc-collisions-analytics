WITH vehicle_join AS (
    SELECT
        vehicle,
        contributing_factor, 
        total_injured, 
        total_killed,
        number_of_persons_injured,
        number_of_persons_killed,
        number_of_cyclist_injured,
        number_of_cyclist_killed,
        number_of_motorist_injured,
        number_of_motorist_killed,
        number_of_pedestrians_injured,
        number_of_pedestrians_killed 
    FROM
        {{ ref('fact_crash_weather_enriched') }},
        UNNEST([vehicle_type_code_1, vehicle_type_code_2, vehicle_type_code_3, vehicle_type_code_4, vehicle_type_code_5]) AS vehicle
        WITH OFFSET AS vehicle_offset,
        UNNEST([contributing_factor_vehicle_1, contributing_factor_vehicle_2, contributing_factor_vehicle_3, contributing_factor_vehicle_4, contributing_factor_vehicle_5]) AS contributing_factor
        WITH OFFSET AS factor_offset
    WHERE
        vehicle_offset = factor_offset AND vehicle is not null
)
SELECT
    vehicle, 
    contributing_factor,
    COUNT(*) AS collision_count,
    SUM(total_injuries) as total_injuries,
    SUM(total_fatalities) as total_fatalities,
    SUM(number_of_persons_injured) as persons_inj_count,
    SUM(number_of_persons_killed) as persons_killed_count, 
    SUM(number_of_cyclist_injured) as cycl_inj_count,
    SUM(number_of_cyclist_killed) as cycl_killed_count, 
    SUM(number_of_motorist_injured) as mot_inj_count, 
    SUM(number_of_motorist_killed) as mot_killed_count, 
    SUM(number_of_pedestrians_injured) as ped_inj_count, 
    SUM(number_of_pedestrians_killed) as ped_killed_count
FROM 
    vehicle_join
GROUP BY 
    vehicle,
    contributing_factor
ORDER BY 
    vehicle, 
    collision_count DESC