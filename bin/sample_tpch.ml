open Core
open Castor
open Db

let main ?(seed = 0) ~db_in ~db_out ~sample =
  let conn = Db.create (sprintf "postgresql:///%s" db_in) in
  Logs.info (fun m -> m "Creating new database %s." db_out) ;
  exec conn ~params:[db_out] "drop database if exists $0" |> ignore ;
  exec conn ~params:[db_out; db_in] "create database $0 with template $1" |> ignore ;
  let conn = Db.create (sprintf "postgresql:///%s" db_out) in
  exec conn ~params:[Int.to_string seed] "set seed to $0" |> ignore ;
  Logs.info (fun m -> m "Sampling %d tuples from lineitem." sample) ;
  exec conn "alter table lineitem rename to old_lineitem" |> ignore ;
  exec conn ~params:[Int.to_string sample]
    "create table lineitem as (select * from old_lineitem order by random() limit \
     $0)"
  |> ignore ;
  exec conn "drop table old_lineitem cascade" |> ignore ;
  Logs.info (fun m -> m "Sampling from table orders.") ;
  exec conn "alter table orders rename to old_orders" |> ignore ;
  exec conn
    {|create table orders as (select * from old_orders where o_orderkey in (select l_orderkey from lineitem))|}
  |> ignore ;
  exec conn "drop table old_orders cascade" |> ignore ;
  Logs.info (fun m -> m "Sampling from table supplier.") ;
  exec conn "alter table supplier rename to old_supplier" |> ignore ;
  exec conn
    {|create table supplier as (select * from old_supplier where s_suppkey in (select l_suppkey from lineitem))|}
  |> ignore ;
  exec conn "drop table old_supplier cascade" |> ignore ;
  Logs.info (fun m -> m "Sampling from table part.") ;
  exec conn "alter table part rename to old_part" |> ignore ;
  exec conn
    {|create table part as (select * from old_part where p_partkey in (select l_partkey from lineitem))|}
  |> ignore ;
  exec conn "drop table old_part cascade" |> ignore ;
  Logs.info (fun m -> m "Sampling from table customer.") ;
  exec conn "alter table customer rename to old_customer" |> ignore ;
  exec conn
    {|create table customer as (select * from old_customer where c_custkey in (select o_custkey from orders))|}
  |> ignore ;
  exec conn "drop table old_customer cascade" |> ignore ;
  Logs.info (fun m -> m "Sampling from table partsupp.") ;
  exec conn "alter table partsupp rename to old_partsupp" |> ignore ;
  exec conn
    {|create table partsupp as (select * from old_partsupp where ps_partkey in (select p_partkey from part) and ps_suppkey in (select s_suppkey from supplier))|}
  |> ignore ;
  exec conn "drop table old_partsupp cascade" |> ignore

let () =
  (* Set early so we get logs from command parsing code. *)
  Logs.set_reporter (Logs.format_reporter ()) ;
  let open Command in
  let open Let_syntax in
  basic ~summary:"Generate a sample database for a benchmark."
    (let%map_open verbose =
       flag "verbose" ~aliases:["v"] no_arg ~doc:"increase verbosity"
     and quiet = flag "quiet" ~aliases:["q"] no_arg ~doc:"decrease verbosity"
     and db_in = flag "i" (required string) ~doc:"DB the input database"
     and db_out = flag "o" (required string) ~doc:"DB the output database"
     and sample =
       flag "n" (required int) ~doc:"SIZE the sample size for the lineitem relation"
     and seed = flag "s" (optional int) ~doc:"the random seed" in
     fun () ->
       if verbose then Logs.set_level (Some Logs.Debug)
       else if quiet then Logs.set_level (Some Logs.Error)
       else Logs.set_level (Some Logs.Info) ;
       main ?seed ~db_in ~db_out ~sample)
  |> run
