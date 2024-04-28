{{ config(materialized='table') }}

WITH monthly_data AS (
    SELECT
        crash_mon_yr,
        SUM(total_persons_injured) AS mon_total_persons_inj, 
        SUM(total_persons_killed) AS mon_total_persons_fatal,
        SUM(number_of_pedestrians_injured) AS mon_total_ped_inj,
        SUM(number_of_pedestrians_killed) AS mon_total_ped_fatal, 
        SUM(number_of_cyclist_injured) AS mon_total_cyc_inj,
        SUM(number_of_cyclist_killed) AS mon_total_cyc_fatal,
        SUM(number_of_motorist_injured) AS mon_total_mot_inj,
        SUM(number_of_motorist_killed) AS mon_total_mot_fatal,
        COUNT(DISTINCT DATE(crash_datetime)) AS days_in_month
    FROM
        {{ ref('fact_crash_weather_enriched') }}
    GROUP BY
        crash_mon_yr
)
SELECT
    crash_mon_yr,
    mon_total_persons_inj,
    mon_total_persons_fatal,
    mon_total_ped_inj,
    mon_total_ped_fatal,
    mon_total_cyc_inj,
    mon_total_cyc_fatal,
    mon_total_mot_inj,
    mon_total_mot_fatal,
    ROUND(mon_total_ped_inj / days_in_month, 1) AS avg_daily_ped_inj,
    ROUND(mon_total_ped_fatal / days_in_month, 1) AS avg_daily_ped_fatal,
    ROUND(mon_total_cyc_inj / days_in_month, 1) AS avg_daily_cyc_inj,
    ROUND(mon_total_cyc_fatal / days_in_month, 1) AS avg_daily_cyc_fatal,
    ROUND(mon_total_mot_inj / days_in_month, 1) AS avg_daily_mot_inj,
    ROUND(mon_total_mot_fatal / days_in_month, 1) AS avg_daily_mot_fatal
FROM
    monthly_data
ORDER BY 
    crash_mon_yr

-- dbt build --m <model.sql> --vars 'is_test_run: false'
--{% if var('is_test_run', default=true) %}

--  limit 100

--{% endif %}