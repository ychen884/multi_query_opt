{{ config(materialized='table') }}

SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    {{ source('tpch', 'customer') }},
    {{ source('tpch', 'orders') }},
    {{ source('tpch', 'lineitem') }},
    {{ source('tpch', 'supplier') }},
    {{ source('tpch', 'nation') }},
    {{ source('tpch', 'region') }}
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC
