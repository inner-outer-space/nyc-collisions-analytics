{% macro get_col_type(schema_name, table_name) %}
    SELECT 
    table_name AS object_name,
    column_name,
    data_type
    FROM `{{ target.project }}.{{ schema_name }}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{{ table_name }}'
{% endmacro %}