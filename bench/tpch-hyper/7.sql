SELECT
    supp_nation,
    cust_nation,
    l_year,
    revenue
FROM
    q7
WHERE
    supp_nation = 'FRANCE'
    AND cust_nation = 'GERMANY'
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

