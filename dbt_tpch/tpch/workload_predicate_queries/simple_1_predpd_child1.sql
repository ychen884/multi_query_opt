{{ config(materialized='table') }}

SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM
    {{ ref('simple_1_predpd_parent') }}
WHERE
    l_shipdate >= CAST('1990-09-02' AS date) AND
    l_commitdate <= CAST('1995-09-02' AS DATE)
    
GROUP BY
    l_returnflag,
    l_linestatus
ORDER BY
    l_returnflag,
    l_linestatus
