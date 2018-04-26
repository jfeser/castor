drop database if exists benchmarks;
create database benchmarks;
\c benchmarks

create type state as enum ('unsearched', 'searching', 'searched');
create table template (
       hash           bigint primary key,
       program_size   bigint,
       valid          boolean,
       search_time    timestamp default localtimestamp,
       search_state   state,
       search_transforms text,
       search_failed  boolean,
       runs_per_sec   numeric,
       db_size        bigint,
       exe_size       bigint,
       compile_failed boolean,
       scanner_failed boolean
);
create index on template (search_time asc) where search_state = 'unsearched';

