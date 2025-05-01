{{ config(materialized='table') }}

WITH 
RevenuePerCustomer AS (
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
),
NationAverage AS (
    SELECT 
        nation,
        AVG(total_revenue) AS avg_revenue
    FROM RevenuePerCustomer
    GROUP BY nation
)
SELECT 
    r.customer_name,
    r.nation,
    r.total_revenue,
    n.avg_revenue
FROM RevenuePerCustomer r
JOIN NationAverage n ON r.nation = n.nation
WHERE r.total_revenue < n.avg_revenue
ORDER BY r.nation, r.total_revenue
