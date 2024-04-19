{% macro calculate_severity(column_prefix='') %}
    CASE
        WHEN {{ column_prefix }}number_of_pedestrians_killed > 0
            OR {{ column_prefix }}number_of_cyclist_killed > 0
            OR {{ column_prefix }}number_of_motorist_killed > 0 THEN 'Fatal'
        WHEN  {{ column_prefix }}number_of_pedestrians_injured > 0
            OR {{ column_prefix }}number_of_cyclist_injured > 0
            OR {{ column_prefix }}number_of_motorist_injured > 0 THEN 'Major'
        ELSE 'Minor'
    END
{% endmacro %}