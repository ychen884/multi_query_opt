SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM {{ ref('simple2_multi_pd_parent') }}
WHERE
    o_orderdate >= CAST('1994-01-01' AS date)
    AND o_orderdate < CAST('1995-01-01' AS date)
    AND l_discount BETWEEN 0.05 AND 0.07
GROUP BY
    n_name
ORDER BY
    revenue DESC