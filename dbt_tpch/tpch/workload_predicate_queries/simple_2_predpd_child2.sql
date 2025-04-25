-- Order Counts by Quarter, Restricted to Europe & 1994
{{ config(materialized='table') }}

SELECT
    CASE
        WHEN EXTRACT(MONTH FROM o_orderdate) BETWEEN 1 AND 3 THEN 'Q1'
        WHEN EXTRACT(MONTH FROM o_orderdate) BETWEEN 4 AND 6 THEN 'Q2'
        WHEN EXTRACT(MONTH FROM o_orderdate) BETWEEN 7 AND 9 THEN 'Q3'
        ELSE 'Q4'
    END AS order_quarter,
    COUNT(*) AS order_count
FROM {{ ref('simple_2_predpd_parent') }}
WHERE
    o_orderdate < '1995-01-01'
    AND o_orderdate >= '1994-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
GROUP BY
    1
