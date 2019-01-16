SELECT
    (sum(l_extendedprice) / 7.0) AS avg_yearly
FROM
    q17, q17_2
WHERE
  p_partkey = l_partkey
  and l_quantity < l_avgquantity
and        p_brand = 'Brand#23'
        AND p_container = 'MED BOX';

