SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM {{ ref('ppd_parent_2') }}
WHERE
    o_orderdate >= CAST('1994-01-01' AS date)
GROUP BY
    n_name
ORDER BY
    revenue DESC