{% macro apply_routing(source_schema, source_table, colspec) %}
with validated as (
    select
        *,
        {{ validate_row(colspec) }} as is_valid
    from {{ source(source_schema, source_table) }}
)
select * from validated
{% endmacro %}
