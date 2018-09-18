((name query99)
 (params ((ds )))
 (query "
Sort([ext_price desc, brand_id desc],
Agg([brand_id := item.i_brand_id, brand := item.i_brand, ext_price := sum(store_sales.ss_ext_sales_price)],
    [item.i_brand, item.i_brand_id],
EqJoin(date_dim.d_date_sk, store_sales.ss_sold_date_sk,
EqJoin(item.i_item_sk, store_sales.ss_item_sk,
Filter(item.i_manager_id = manager:int, item),
store_sales),
Filter(date_dim.d_moy = month:int && date_dim.d_year = year:int,
date_dim))))
"
  )
 (sql "
select i_brand_id brand_id, i_brand brand, sum(ss_ext_sales_price) ext_price
from date_dim, store_sales, item
where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=[MANAGER]
 	and d_moy=[MONTH]
 	and d_year=[YEAR]
group by i_brand, i_brand_id
order by ext_price desc, i_brand_id
limit 100;
"))

