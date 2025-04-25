{{ config(materialized='table') }}

SELECT * 
FROM {{ source('tpch', 'lineitem') }}

