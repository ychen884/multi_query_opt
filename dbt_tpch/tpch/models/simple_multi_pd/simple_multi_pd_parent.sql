{{ config(materialized='table') }}

SELECT * 
FROM {{ source('tpch', 'lineitem') }}
WHERE l_extendedprice > 0
