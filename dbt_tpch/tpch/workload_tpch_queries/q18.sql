{{ config(materialized='table') }}

SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
FROM
    {{ source('tpch', 'customer') }},
    {{ source('tpch', 'orders') }},
    {{ source('tpch', 'lineitem') }}
WHERE
    o_orderkey IN (
        SELECT
            l_orderkey
        FROM
            {{ source('tpch', 'lineitem') }}
        GROUP BY
            l_orderkey
        HAVING
            sum(l_quantity) > 300)
    AND c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
ORDER BY
    o_totalprice DESC,
    o_orderdate
LIMIT 100
