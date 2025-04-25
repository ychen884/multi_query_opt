--  Revenue by Customer & Supplier
{{ config(materialized='table') }}

SELECT
    c_custkey,
    s_suppkey,
    SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
FROM {{ ref('simple_2_predpd_parent') }} 
WHERE
    o_orderdate >= '1994-01-01'
    AND o_orderdate < '1995-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
GROUP BY
    c_custkey,
    s_suppkey
