open Prim_type

module Expect_test_config = struct
  include Expect_test_config

  let run thunk =
    Fresh.reset Global.fresh;
    thunk ();
    ()
end

let pg_tmp = Sys.getenv_exn "CASTOR_ROOT" ^ "/vendor/ephemeralpg/pg_tmp"

let create_test_db () =
  let ch = Unix.open_process_in pg_tmp in
  let url = In_channel.input_all ch in
  Unix.close_process_in ch |> Unix.Exit_or_signal.or_error |> Or_error.ok_exn;
  Db.create url

(** Create a simple database table that only contains integers. *)
let create_simple db name fields values =
  let fields_sql =
    List.map fields ~f:(sprintf "%s integer") |> String.concat ~sep:", "
  in
  Db.(
    psql_exec db (sprintf "create table if not exists %s (%s)" name fields_sql)
    |> Or_error.ok_exn |> command_ok_exn);
  List.iter values ~f:(fun vs ->
      let values_sql =
        List.map vs ~f:Int.to_string |> String.concat ~sep:", "
      in
      Db.(
        psql_exec db (sprintf "insert into %s values (%s)" name values_sql)
        |> Or_error.ok_exn |> command_ok_exn))

let create db name fields values =
  let fields_sql =
    List.map fields ~f:(fun (f, t) ->
        match t with
        | FixedT _ -> sprintf "%s numeric" f
        | IntT _ -> sprintf "%s integer" f
        | StringT _ -> sprintf "%s varchar" f
        | BoolT _ -> sprintf "%s boolean" f
        | DateT _ -> sprintf "%s date" f
        | _ -> failwith "Unexpected type.")
    |> String.concat ~sep:", "
  in
  Db.(
    psql_exec db (sprintf "create table %s (%s)" name fields_sql)
    |> Or_error.ok_exn |> command_ok_exn);
  List.iter values ~f:(fun vs ->
      let values_sql = List.map vs ~f:Value.to_sql |> String.concat ~sep:", " in
      Db.(
        psql_exec db (sprintf "insert into %s values (%s)" name values_sql)
        |> Or_error.ok_exn |> command_ok_exn))

let test_db_conn =
  lazy
    (let conn = create_test_db () in
     create_simple conn "r" [ "f"; "g" ]
       [ [ 0; 5 ]; [ 1; 2 ]; [ 1; 3 ]; [ 2; 1 ]; [ 2; 2 ]; [ 3; 4 ]; [ 4; 6 ] ];
     create conn "r_date"
       [ ("f", date_t); ("g", int_t) ]
       Date.
         [
           [ Date (of_string "2018-01-01"); Int 5 ];
           [ Date (of_string "2016-12-01"); Int 2 ];
           [ Date (of_string "2018-01-23"); Int 3 ];
           [ Date (of_string "2017-10-05"); Int 1 ];
           [ Date (of_string "2018-01-01"); Int 2 ];
           [ Date (of_string "2018-09-01"); Int 4 ];
           [ Date (of_string "2018-01-01"); Int 6 ];
         ];
     create_simple conn "s" [ "f"; "g" ]
       [ [ 0; 5 ]; [ 1; 2 ]; [ 1; 3 ]; [ 2; 1 ]; [ 2; 2 ]; [ 3; 4 ]; [ 4; 6 ] ];
     create_simple conn "log"
       [ "counter"; "succ"; "id" ]
       [ [ 1; 4; 1 ]; [ 2; 3; 2 ]; [ 3; 4; 3 ]; [ 4; 6; 1 ]; [ 5; 6; 3 ] ];
     create_simple conn "r1" [ "f"; "g" ]
       [ [ 1; 2 ]; [ 1; 3 ]; [ 2; 1 ]; [ 2; 2 ]; [ 3; 4 ] ];
     create_simple conn "one" [] [];
     create conn "r2" [ ("a", fixed_t) ]
       [
         [ Fixed (Fixed_point.of_string "0.01") ];
         [ Fixed (Fixed_point.of_string "5") ];
         [ Fixed (Fixed_point.of_string "34.42") ];
         [ Fixed (Fixed_point.of_string "0.88") ];
         [ Fixed (Fixed_point.of_string "-0.42") ];
       ];
     create conn "log_str"
       [ ("counter", int_t); ("succ", int_t); ("id", string_t) ]
       [
         [ Int 1; Int 4; String "foo" ];
         [ Int 2; Int 3; String "fizzbuzz" ];
         [ Int 3; Int 4; String "bar" ];
         [ Int 4; Int 6; String "foo" ];
         [ Int 5; Int 6; String "bar" ];
       ];
     create conn "unique_str"
       [ ("str_field", string_t) ]
       [ [ String "a" ]; [ String "b" ]; [ String "c" ] ];
     create conn "ints" [ ("x", int_t) ]
       (List.init 10 ~f:(fun i -> [ Value.Int i ]));
     conn)

module Demomatch = struct
  let example_params =
    [
      (Name.create "id_p", Prim_type.int_t, Value.Int 1);
      (Name.create "id_c", Prim_type.int_t, Int 2);
    ]

  let example_str_params =
    [
      (Name.create "id_p", Prim_type.string_t, Value.String "foo");
      (Name.create "id_c", Prim_type.string_t, String "fizzbuzz");
    ]

  let example_db_params =
    [
      ( Name.create "id_p",
        Prim_type.string_t,
        Value.String "-1451410871729396224" );
      (Name.create "id_c", Prim_type.string_t, String "8557539814359574196");
    ]

  let example1 log =
    sprintf
      {|
select([p_counter, c_counter], filter(c_id = id_c && p_id = id_p,
alist(filter(succ > counter + 1, %s) as lp,
atuple([ascalar(lp.id as p_id), ascalar(lp.counter as p_counter),
alist(filter(lp.counter < counter && counter < lp.succ, %s) as lc,
atuple([ascalar(lc.id as c_id), ascalar(lc.counter as c_counter)], cross))], cross))))
|}
      log log

  let example2 log =
    sprintf
      {|
select([p_counter, c_counter],
ahashidx(dedup(
      join(true, select([id as p_id], %s), select([id as c_id], %s))) as k,
  alist(select([p_counter, c_counter],
    join(p_counter < c_counter && c_counter < p_succ,
      filter(p_id = k.p_id,
        select([id as p_id, counter as p_counter, succ as p_succ], %s)), 
      filter(c_id = k.c_id,
        select([id as c_id, counter as c_counter], %s)))) as lk,
    atuple([ascalar(lk.p_counter), ascalar(lk.c_counter)], cross)),
  (id_p, id_c)))
|}
      log log log log

  let example3 log =
    sprintf
      {|
select([p_counter, c_counter],
  depjoin(
    ahashidx(dedup(select([id as p_id], %s)) as hk,
    alist(select([counter, succ], filter(hk.p_id = id && counter < succ, %s)) as lk1, 
      atuple([ascalar(lk1.counter as p_counter), ascalar(lk1.succ as p_succ)], cross)), 
    id_p) as dk,
  select([dk.p_counter, c_counter],
  filter(c_id = id_c,
    aorderedidx(select([counter], %s) as ok, 
      alist(filter(counter = ok.counter, %s) as lk2,
        atuple([ascalar(lk2.id as c_id), ascalar(lk2.counter as c_counter)], cross)), 
      dk.p_counter, dk.p_succ)))))
|}
      log log log log
end

let sum_complex =
  "Select([sum(f) + 5, count() + sum(f / 2)], AList(r1 as k, \
   ATuple([AScalar(k.f), AScalar((k.g - k.f) as v)], cross)))"

let tpch_conn = lazy (Db.create @@ Sys.getenv_exn "CASTOR_TPCH_TEST_DB")

let tpch_full_conn = lazy (Db.create @@ Sys.getenv_exn "CASTOR_TPCH_DB")
