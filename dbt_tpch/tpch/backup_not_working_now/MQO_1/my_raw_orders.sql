{{ config(materialized='table') }}

-- This is our base table of orders. We'll just hard-code a small dataset.
select 1 as order_id, 2 as customer_id, 100.0 as amount, '2023-01-01' as order_date
union all
select 2, 2, 50.0, '2023-01-02'
union all
select 3, 3, 75.0, '2022-12-31'
