{% macro apply_transformations(colspec) %}
    {% for col, type in colspec.items() %}
        {% if type == "integer" %}
            {{ clean_integer(col) }} as {{ col }}
        {% elif type == "decimal" %}
            {{ clean_decimal(col) }} as {{ col }}
        {% elif type == "date" %}
            {{ clean_date(col) }} as {{ col }}
        {% else %}
            {{ col }} as {{ col }}
        {% endif %}
        {% if not loop.last %}, {% endif %}
    {% endfor %}
{% endmacro %}
