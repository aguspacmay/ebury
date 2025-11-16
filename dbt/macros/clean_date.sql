{% macro clean_date(column_name) %}
(
    case 
        -- Intenta YYYY-MM-DD
        when regexp_replace({{ column_name }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{4}-\d{2}-\d{2}$'
        then to_date(regexp_replace({{ column_name }}, '[^0-9/:\- ]', '', 'g'), 'YYYY-MM-DD')
        
        -- Intenta DD-MM-YYYY
        when regexp_replace({{ column_name }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{2}-\d{2}-\d{4}$'
        then to_date(regexp_replace({{ column_name }}, '[^0-9/:\- ]', '', 'g'), 'DD-MM-YYYY')
        
        -- Intenta DD/MM/YYYY
        when regexp_replace({{ column_name }}, '[^0-9/:\- ]', '', 'g') ~ '^\d{2}/\d{2}/\d{4}$'
        then to_date(regexp_replace({{ column_name }}, '[^0-9/:\- ]', '', 'g'), 'DD/MM/YYYY')
        
        else null
    end
)
{% endmacro %}
