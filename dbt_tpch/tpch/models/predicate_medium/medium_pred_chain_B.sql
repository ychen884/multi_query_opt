{{ config(materialized='table') }}

SELECT
    C.*,
    l3.l_partkey AS extra_partkey_column
FROM {{ ref('medium_pred_chain_C') }} AS C
JOIN {{ source('tpch', 'lineitem') }} AS l3
    ON C.l_orderkey = l3.l_orderkey
WHERE
    l3.l_shipdate < CAST('1995-01-01' AS DATE)
    AND l3.l_shipdate >= CAST('1990-01-01' AS DATE)
    AND l3.l_discount < 0.05
    AND l3.l_discount >= 0.01