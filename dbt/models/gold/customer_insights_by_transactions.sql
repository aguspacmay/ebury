-- models/gold/customer_insights_by_transactions.sql
{{ config(
    materialized='table',
    schema='gold',
    alias='customer_insights_by_transactions',
    unique_key=['customer_id', 'transaction_year', 'transaction_month'],
    on_schema_change='fail'
    ) 
}}

WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(price) AS total_spent,
        SUM(quantity) AS total_items,
        COUNT(DISTINCT transaction_id) AS total_transactions
    FROM {{ ref('facts_transactions') }}
    GROUP BY customer_id
), product_preferences AS (
    SELECT
        customer_id,
        product_id,
        SUM(quantity) AS total_quantity
    FROM {{ ref('facts_transactions') }}
    GROUP BY customer_id, product_id
), favorite_products AS (
    SELECT
        customer_id,
        product_id AS favorite_product_id,
        total_quantity AS favorite_product_quantity,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_quantity DESC) AS rn
    FROM product_preferences
)
SELECT
    ct.customer_id,
    ct.total_spent,
    ct.total_items,
    ct.total_transactions,
    fp.favorite_product_id,
    fp.favorite_product_quantity
FROM customer_totals ct
LEFT JOIN favorite_products fp
    ON ct.customer_id = fp.customer_id AND fp.rn = 1