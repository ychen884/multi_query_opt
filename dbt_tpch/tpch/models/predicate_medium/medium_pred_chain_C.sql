{{ config(materialized='table') }}

SELECT
    D.*,
    l2.l_discount AS extra_discount_column
FROM {{ ref('medium_pred_chain_D') }} AS D
JOIN {{ source('tpch', 'lineitem') }} AS l2
    ON D.l_orderkey = l2.l_orderkey
WHERE
    l2.l_shipdate < CAST('1995-01-01' AS DATE)
    AND l2.l_shipdate >= CAST('1990-01-01' AS DATE)
    AND l2.l_discount < 0.05
    AND l2.l_discount >= 0.01