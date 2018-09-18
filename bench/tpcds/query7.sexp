 ;; define GEN= dist(gender, 1, 1);
 ;; define MS= dist(marital_status, 1, 1);
 ;; define ES= dist(education, 1, 1);
 ;; define YEAR = random(1998,2002,uniform);
 ;; define _LIMIT=100;
 
 ;; [_LIMITA] select [_LIMITB] i_item_id, 
 ;;        avg(ss_quantity) agg1,
 ;;        avg(ss_list_price) agg2,
 ;;        avg(ss_coupon_amt) agg3,
 ;;        avg(ss_sales_price) agg4 
 ;; from store_sales, customer_demographics, date_dim, item, promotion
 ;; where ss_sold_date_sk = d_date_sk and
 ;;       ss_item_sk = i_item_sk and
 ;;       ss_cdemo_sk = cd_demo_sk and
 ;;       ss_promo_sk = p_promo_sk and
 ;;       cd_gender = '[GEN]' and 
 ;;       cd_marital_status = '[MS]' and
 ;;       cd_education_status = '[ES]' and
 ;;       (p_channel_email = 'N' or p_channel_event = 'N') and
 ;;       d_year = [YEAR] 
 ;; group by i_item_id
 ;; order by i_item_id
 ;; [_LIMITC];

orderby([i_item_id],
  groupby([i_item_id,
           avg(ss_quantity),
           avg(ss_list_price),
           avg(ss_coupon_amt),
           avg(ss_sales_price)],
          [i_item_id],
    join(ss_sold_date_sk = d_date_sk,
      join(ss_item_sk = i_item_sk,
        join(ss_cdemo_sk = cd_demo_sk,
          join(ss_promo_sk = p_promo_sk,
            store_sales,
            filter(p_channel_email = "N" || p_channel_event = "N", promotion),
          filter(cd_gender = param0 &&
                cd_marital_status = param1 &&
                cd_education_status = param2,
            customer_demographics)),
        item),
      filter(d_year = param3, date_dim))))
