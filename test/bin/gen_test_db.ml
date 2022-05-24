open Prim_type

(** Create a simple database table that only contains integers. *)
let create_simple db name fields values =
  let fields_sql =
    List.map fields ~f:(sprintf "%s integer") |> String.concat ~sep:", "
  in
  Db.(
    psql_exec db (sprintf "drop table if exists %s" name)
    |> Or_error.ok_exn |> command_ok_exn);
  Db.(
    psql_exec db (sprintf "create table %s (%s)" name fields_sql)
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
    psql_exec db (sprintf "drop table if exists %s" name)
    |> Or_error.ok_exn |> command_ok_exn);
  Db.(
    psql_exec db (sprintf "create table %s (%s)" name fields_sql)
    |> Or_error.ok_exn |> command_ok_exn);
  List.iter values ~f:(fun vs ->
      let values_sql = List.map vs ~f:Value.to_sql |> String.concat ~sep:", " in
      Db.(
        psql_exec db (sprintf "insert into %s values (%s)" name values_sql)
        |> Or_error.ok_exn |> command_ok_exn))

let () =
  Db.with_conn "postgresql:///castor_test" @@ fun conn ->
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
  create conn "r2"
    [ ("a", fixed_t) ]
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
  create conn "ints"
    [ ("x", int_t) ]
    (List.init 10 ~f:(fun i -> [ Value.Int i ]))
