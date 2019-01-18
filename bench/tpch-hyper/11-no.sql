SELECT
    ps_partkey,
    value_
FROM
    q11_2,
    q11_1
WHERE
    q11_1.n_name = q11_2.n_name
    AND value_ > (const33 * 0.0001)
    AND q11_1.n_name = 'GERMANY';

