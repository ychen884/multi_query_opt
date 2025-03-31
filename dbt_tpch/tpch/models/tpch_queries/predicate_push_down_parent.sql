{{ config(materialized='table') }}

SELECT *
FROM
    {{ source('tpch', 'lineitem') }}
WHERE
    l_shipdate <= CAST('1998-09-02' AS date)
