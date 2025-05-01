{{ config(materialized='table') }}

SELECT
    l_orderkey,
    l_shipdate,
    l_extendedprice
FROM {{ source('tpch', 'lineitem') }} AS l
WHERE
    l_shipdate < CAST('2001-01-01' AS DATE)
    AND l_discount < 0.7

-- we hope to push down l_discount >= 0.01, < 0.05, l_shipdate 1990-01-01 ~ 1995-01-01