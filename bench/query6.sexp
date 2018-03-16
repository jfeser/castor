 ; define YEAR = random(1998, 2002, uniform);
 ; define MONTH= random(1,7,uniform);
 ; define _LIMIT=100;
 
 ; [_LIMITA] select [_LIMITB] a.ca_state state, count(*) cnt
 ; from customer_address a
 ;     ,customer c
 ;     ,store_sales s
 ;     ,date_dim d
 ;     ,item i
 ; where       a.ca_address_sk = c.c_current_addr_sk
 ; 	and c.c_customer_sk = s.ss_customer_sk
 ; 	and s.ss_sold_date_sk = d.d_date_sk
 ; 	and s.ss_item_sk = i.i_item_sk
 ; 	and d.d_month_seq = 
 ; 	     (select distinct (d_month_seq)
 ; 	      from date_dim
 ;               where d_year = [YEAR]
 ; 	        and d_moy = [MONTH] )
 ; 	and i.i_current_price > 1.2 * 
 ;             (select avg(j.i_current_price) 
 ; 	     from item j 
 ; 	     where j.i_category = i.i_category)
 ; group by a.ca_state
 ; having count(*) >= 10
 ; order by cnt, a.ca_state 
 ; [_LIMITC];

((name query6)
 (params
  ((year ((Int 1998) (Int 1999) (Int 2000) (Int 2001) (Int 2002)))
   (month ((Int 1) (Int 2) (Int 3) (Int 4) (Int 5) (Int 6) (Int 7))))
  (query "

"))
 )
