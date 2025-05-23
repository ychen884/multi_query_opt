{{ config(materialized='table') }}

SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM
    {{ source('tpch', 'lineitem') }},
    {{ source('tpch', 'part') }}
WHERE
    p_partkey = l_partkey
    AND p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT
            0.2 * avg(l_quantity)
        FROM
            {{ source('tpch', 'lineitem') }}
        WHERE
            l_partkey = p_partkey)
