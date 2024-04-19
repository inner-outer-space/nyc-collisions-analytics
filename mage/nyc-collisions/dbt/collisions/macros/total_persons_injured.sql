{% macro total_persons_injured() %}
    SELECT
        COALESCE(number_of_pedestrians_injured, 0) 
        + COALESCE(number_of_cyclist_injured, 0) 
        + COALESCE(number_of_motorist_injured, 0) AS total_persons_injured
{% endmacro %}