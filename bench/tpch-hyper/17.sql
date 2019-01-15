select (sum(l_extendedprice) / 7.0) as avg_yearly from q17 where l_quantity < (select l_avgquantity from q17_2 where p_partkey = l_partkey) where p_brand = 'Brand#23' and p_container = 'MED BOX';
