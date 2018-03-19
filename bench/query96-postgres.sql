-- set seed to 0;
-- create temp table store_sales as (select * from store_sales order by random() limit 10000);
-- set seed to 0;
-- create temp table household_demographics as (select * from household_demographics order by random() limit 10000);
-- set seed to 0;
-- create temp table time_dim as (select * from time_dim order by random() limit 10000);
-- set seed to 0;
-- create temp table store as (select * from store order by random() limit 10000);

-- Run with no view and no indexes.
select count(*) from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
and ss_hdemo_sk = household_demographics.hd_demo_sk
and ss_store_sk = s_store_sk
and time_dim.t_hour = 20
and time_dim.t_minute >= 30
and household_demographics.hd_dep_count = 0
and store.s_store_name = 'ese'
order by count(*);

explain analyze select count(*) from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
and ss_hdemo_sk = household_demographics.hd_demo_sk
and ss_store_sk = s_store_sk
and time_dim.t_hour = 20
and time_dim.t_minute >= 30
and household_demographics.hd_dep_count = 0
and store.s_store_name = 'ese'
order by count(*);

-- Run with view but no indexes.
create temp table v1 as (
select t_hour, hd_dep_count from store_sales, household_demographics, time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk
and ss_hdemo_sk = household_demographics.hd_demo_sk
and ss_store_sk = s_store_sk
and time_dim.t_minute >= 30
and store.s_store_name = 'ese');

select count(*) from v1 where
v1.t_hour = 20
and v1.hd_dep_count = 0;

explain analyze select count(*) from v1 where
v1.t_hour = 20
and v1.hd_dep_count = 0;

create index on v1 (t_hour);
create index on v1 (hd_dep_count);

select count(*) from v1 where
v1.t_hour = 20
and v1.hd_dep_count = 0;

explain analyze select count(*) from v1 where
v1.t_hour = 20
and v1.hd_dep_count = 0;


