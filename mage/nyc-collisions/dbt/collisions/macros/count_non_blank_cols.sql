{% macro count_non_blank_cols(prefix, num_fields) %}
    {% set case_statements = [] %}
    {% for i in range(1, num_fields + 1) %}
        {% set field_name = prefix ~ i %}
        {% set case_statement = "CASE WHEN " ~ field_name ~ " IS NOT NULL AND " ~ field_name ~ " != '' THEN 1 ELSE 0 END AS non_blank_" ~ i %}
        {% do case_statements.append(case_statement) %}
    {% endfor %}
    {{ case_statements | join(", ") }}
{% endmacro %}


--SELECT
--    vehicle_type_code_1,
--    vehicle_type_code_2,
--    vehicle_type_code_3,
--    vehicle_type_code_4,
--    vehicle_type_code_5,
--     {{ count_non_blank_cols('vehicle_type_code_', 5) }} AS non_blank_count