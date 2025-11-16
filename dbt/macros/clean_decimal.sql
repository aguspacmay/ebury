{% macro clean_decimal(column_name) %}
nullif(
    regexp_replace({{ column_name }}, '[^0-9\.-]', '', 'g'),
    ''
)::numeric
{% endmacro %}
