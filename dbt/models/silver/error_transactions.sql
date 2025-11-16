-- models/error_transactions.sql
{{ config(
    materialized='incremental',
    schema='error',
    alias='customer_transactions',
    unique_key='transaction_id',
    on_schema_change='fail'
) }}

{% set spec = var("column_spec") %}


with routing as (
    {{ apply_routing('ebury_bronze', 'customer_transactions', spec) }}
),

with_timestamp as (
    SELECT
        *,
        now() as error_timestamp
    FROM routing
    WHERE is_valid = false
)

SELECT * FROM with_timestamp

{% if is_incremental() %}
  -- Solo procesa transaction_ids que no existen en la tabla destino
  -- transaction_id en error es TEXT (no se transforma)
  WHERE transaction_id NOT IN (SELECT transaction_id FROM {{ this }})
{% endif %}
