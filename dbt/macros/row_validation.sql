{% macro validate_row(colspec) %}
(
    {% for col, type in colspec.items() %}
        {% if type == "integer" %}
            ({{ col }} is not null AND trim({{ col }}) != '' AND regexp_replace({{ col }}, '[^0-9\.-]', '', 'g') ~ '^-?[0-9]+(\.0)?$')
        {% elif type == "decimal" %}
            ({{ col }} is not null AND trim({{ col }}) != '' AND regexp_replace({{ col }}, '[^0-9\.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$')
        {% elif type == "date" %}
            (
                {{ col }} is not null 
                AND trim({{ col }}) != ''
                AND (
                    (regexp_replace({{ col }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{4}-\d{2}-\d{2}$')
                    OR (regexp_replace({{ col }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{4}/\d{2}/\d{2}$')
                    OR (regexp_replace({{ col }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{2}-\d{2}-\d{4}$')
                    OR (regexp_replace({{ col }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{2}/\d{2}/\d{4}$')
                )
            )
        {% else %}
            true
        {% endif %}
        {% if not loop.last %} AND {% endif %}
    {% endfor %}
)
{% endmacro %}
