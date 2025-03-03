{{ config(materialized='table') }}

-- This model also filters out old rows, then does a daily sum of 'amount'.
select
    order_date,
    sum(amount) as total
from {{ ref('my_raw_orders') }}
where order_date >= '2023-01-01'
group by order_date
