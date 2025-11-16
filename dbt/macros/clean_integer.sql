{% macro clean_integer(column_name) %}
nullif(
    regexp_replace(
        regexp_replace({{ column_name }}, '[^0-9\.-]', '', 'g'),
        '\.0$', '', 'g'
    ),
    ''
)::integer
{% endmacro %}
