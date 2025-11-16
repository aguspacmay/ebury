{% macro strip_non_numeric(value) %}
regexp_replace({{ value }}, '[^0-9]', '', 'g')
{% endmacro %}
