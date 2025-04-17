SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM {{ ref('ppd_parent_2') }}
WHERE
    l_discount BETWEEN 0.05 AND 0.07
GROUP BY
    n_name
ORDER BY
    revenue DESC