-- models/gold/customer_totals_by_month.sql
{{ config(
    materialized='table',
    schema='gold',
    alias='customer_totals_by_month',
    unique_key=['customer_id', 'transaction_year', 'transaction_month'],
    on_schema_change='sync_all_columns'
    ) 
}}

SELECT
    customer_id,
    DATE_PART('year', transaction_date) AS transaction_year,
    DATE_PART('month', transaction_date) AS transaction_month,
    SUM(price) AS total_spent,
    SUM(quantity) AS total_items,
    ROUND( AVG(price), 2) AS avg_spent_per_transaction,
    COUNT(DISTINCT transaction_id) AS total_transactions
FROM {{ ref('facts_transactions') }}
GROUP BY customer_id, DATE_PART('year', transaction_date), DATE_PART('month', transaction_date)
