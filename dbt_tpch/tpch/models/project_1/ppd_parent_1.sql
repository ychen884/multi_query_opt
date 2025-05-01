{{ config(materialized='table') }}

SELECT *
FROM
    {{ source('tpch', 'lineitem') }}
where l_shipdate <= CAST('2005-09-02' AS date) 