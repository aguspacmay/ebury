-- models/silver/validated_transactions.sql
{{ config(
    materialized='incremental',
    schema='silver',
    alias='customer_transactions',
    unique_key='transaction_id',
    on_schema_change='fail'
    ) 
}}

{% set spec = var("column_spec") %}


with routing as (
    {{ apply_routing('ebury_bronze', 'customer_transactions', spec) }}
),

transformed as (
    SELECT 
        {{ apply_transformations(spec) }}
    FROM routing
    WHERE is_valid = true
)

SELECT * FROM transformed

{% if is_incremental() %}
  -- Solo procesa transaction_ids que no existen en la tabla destino
  WHERE transaction_id NOT IN (SELECT transaction_id FROM {{ this }})
{% endif %}