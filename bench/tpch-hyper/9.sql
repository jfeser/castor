select nation, o_year, sum(amount) as sum_profit from q9
 where case
       when ':1' = 'yellow' then wit1_11
       when ':1' = 'white' then wit1_10
       when ':1' = 'red' then wit1_9
       when ':1' = 'purple' then wit1_8
       when ':1' = 'pink' then wit1_7
       when ':1' = 'orange' then wit1_6
       when ':1' = 'navy' then wit1_5
       when ':1' = 'grey' then wit1_4
       when ':1' = 'green' then wit1_3
       when ':1' = 'brown' then wit1_2
       when ':1' = 'blue' then wit1_1
       when ':1' = 'black' then wit1_0
       else strpos(p_name, 'green') > 0
       end
 order by nation, o_year;


