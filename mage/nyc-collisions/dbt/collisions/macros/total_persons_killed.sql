{% macro total_persons_killed() %}
    SELECT
        COALESCE(number_of_pedestrians_killed, 0) 
        + COALESCE(number_of_cyclist_killed, 0) 
        + COALESCE(number_of_motorist_killed, 0) AS total_persons_killed
{% endmacro %}