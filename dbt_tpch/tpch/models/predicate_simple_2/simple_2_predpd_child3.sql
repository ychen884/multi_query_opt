{{ config(materialized='table') }}

SELECT
    l_shipmode,
    AVG(l_quantity) AS avg_quantity,
    MIN(l_quantity) AS min_quantity,
    MAX(l_quantity) AS max_quantity,
    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue,
    COUNT(DISTINCT l_orderkey) AS distinct_orders
FROM {{ ref('simple_2_predpd_parent') }}
WHERE
    l_discount BETWEEN 0.05 AND 0.07
    AND o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
GROUP BY
    l_shipmode
ORDER BY
    total_revenue DESC