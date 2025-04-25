{{ config(materialized='table') }}

WITH revenue AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM
        {{ source('tpch', 'lineitem') }}
    WHERE
        l_shipdate >= CAST('1996-01-01' AS date)
      AND l_shipdate < CAST('1996-04-01' AS date)
    GROUP BY
        supplier_no
),
wow AS (
    SELECT 'hello' AS hello
)
SELECT
    c_name,
    sum(o_totalprice) AS total_customer_price,
    (SELECT hello FROM wow) AS hello
FROM
    {{ source('tpch', 'customer') }},
    {{ source('tpch', 'orders') }},
    revenue
WHERE
    o_custkey = c_custkey AND
    o_orderdate >= CAST('1996-01-01' AS date) AND
    o_orderdate < CAST('1996-04-01' AS date)
GROUP BY
    c_name,
    hello
HAVING
    sum(o_totalprice) > (
        SELECT
            max(total_revenue)
        FROM revenue)