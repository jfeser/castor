#!/bin/sh
set -e
psql --dbname "tpch" --username "$POSTGRES_USER" <<-EOSQL
drop database if exists tpch_1k;
create database tpch_1k with template tpch;
set seed to 0;
alter table lineitem rename to old_lineitem;
create table lineitem as (select * from old_lineitem order by random() limit 1000);
drop table old_lineitem cascade;
alter table orders rename to old_orders;
create table orders as (select * from old_orders where o_orderkey in (select l_orderkey from lineitem));
drop table old_orders cascade;
alter table supplier rename to old_supplier;
create table supplier as (select * from old_supplier where s_suppkey in (select l_suppkey from lineitem));
drop table old_supplier cascade;
alter table part rename to old_part;
create table part as (select * from old_part where p_partkey in (select l_partkey from lineitem));
drop table old_part cascade;
alter table customer rename to old_customer;
create table customer as (select * from old_customer where c_custkey in (select o_custkey from orders));
drop table old_customer cascade;
alter table partsupp rename to old_partsupp;
create table partsupp as (select * from old_partsupp where ps_partkey in (select p_partkey from part) and ps_suppkey in (select s_suppkey from supplier));
drop table old_partsupp cascade;
EOSQL
