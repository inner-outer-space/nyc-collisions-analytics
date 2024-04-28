{% macro count_per_category(column_name) %}
    SELECT
        SUM(CASE WHEN {{ column_name }} = 'fatal' THEN 1 ELSE 0 END) AS count_fatal,
        SUM(CASE WHEN {{ column_name }} = 'severe' THEN 1 ELSE 0 END) AS count_severe,
        SUM(CASE WHEN {{ column_name }} = 'none' THEN 1 ELSE 0 END) AS count_none
    FROM
        {{ this }}
{% endmacro %}


-- {{ count_per_category('severity') }}