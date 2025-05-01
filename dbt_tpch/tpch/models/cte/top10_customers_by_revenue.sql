{{ config(materialized='table') }}

WITH RevenuePerCustomer AS (
    SELECT 
        c.c_custkey AS custkey,
        c.c_name AS customer_name,
        n.n_name AS nation,
        SUM(l.l_extendedprice * (1 - l.l_discount)) AS total_revenue
    FROM {{ source('tpch', 'lineitem') }} l
    JOIN {{ source('tpch', 'orders') }} o ON l.l_orderkey = o.o_orderkey
    JOIN {{ source('tpch', 'customer') }} c ON o.o_custkey = c.c_custkey
    JOIN {{ source('tpch', 'nation') }} n ON c.c_nationkey = n.n_nationkey
    GROUP BY c.c_custkey, c.c_name, n.n_name
)
SELECT 
    customer_name,
    nation,
    total_revenue
FROM RevenuePerCustomer
ORDER BY total_revenue DESC
LIMIT 10