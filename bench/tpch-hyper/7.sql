SELECT
    n1.n_name,
    n2.n_name,
    l_year
FROM
    q7
WHERE
    n1.n_name = :1
    AND n2.n_name = :2
ORDER BY
    n1.n_name,
    n2.n_name,
    l_year;

