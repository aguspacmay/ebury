-- models/gold/facts_transactions.sql
{{ config(
    materialized='incremental',
    schema='gold',
    alias='facts_transactions',
    unique_key='transaction_id',
    on_schema_change='fail'
    ) 
}}

SELECT
    transaction_id,
    customer_id,
    product_id,
    transaction_date,
    quantity,
    price,
    tax
FROM {{ ref('validated_transactions') }}

{% if is_incremental() %}
WHERE transaction_id NOT IN (SELECT transaction_id FROM {{ this }})
{% endif %}