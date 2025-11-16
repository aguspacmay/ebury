-- models/gold/dim_product.sql
{{ config(
    materialized='incremental',
    schema='gold',
    alias='dim_product',
    unique_key='product_id',
    on_schema_change='fail'
    ) 
}}


SELECT DISTINCT
    product_id,
    product_name
FROM {{ ref('validated_transactions') }}


{% if is_incremental() %}
WHERE product_id NOT IN (SELECT product_id FROM {{ this }})
{% endif %}