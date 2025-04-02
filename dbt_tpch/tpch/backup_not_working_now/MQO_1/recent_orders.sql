{{ config(materialized='table') }}

-- This model selects only 'recent' orders (>= 2023-01-01)
select
    order_id,
    customer_id,
    amount,
    order_date
from {{ ref('my_raw_orders') }}
where order_date >= '2023-01-01'
