{{ config(materialized='table') }}

SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge
FROM
    {{ ref('ppd_parent_1') }}
WHERE
    l_shipdate >= CAST('1990-09-02' AS date)
    AND l_commitdate <= CAST('1995-09-02' AS DATE) 
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus