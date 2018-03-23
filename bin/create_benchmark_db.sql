drop database if exists benchmarks;
create database benchmarks;
\c benchmarks

create type state as enum ('unsearched', 'searching', 'searched');
create table template (
       hash           integer primary key,
       program_size   integer,
       valid          boolean,
       search_time    timestamp default localtimestamp,
       search_state   state,
       search_transforms text,
       search_failed  boolean,
       runs_per_sec   numeric,
       db_size        integer,
       exe_size       integer,
       compile_failed boolean,
       scanner_failed boolean
);
create index on template (search_time asc) where search_state = 'unsearched';

