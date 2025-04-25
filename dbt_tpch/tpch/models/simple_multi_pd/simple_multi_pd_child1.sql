{{ config(materialized='table') }}

SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM
    {{ ref('simple_multi_pd_parent') }}
WHERE
    l_shipdate >= CAST('1994-01-01' AS date)
    AND l_shipdate < CAST('1995-01-01' AS date)
    AND l_extendedprice > 0
    AND l_quantity < 24
