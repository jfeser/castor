open Core
open Printf

exception TestDbExn

let run_in_fork thunk =
  match Unix.fork () with
  | `In_the_child ->
      Backtrace.elide := false ;
      Logs.set_reporter (Logs.format_reporter ()) ;
      Logs.set_level (Some Logs.Debug) ;
      thunk () ;
      exit 0
  | `In_the_parent pid ->
      let _, err = Unix.wait (`Pid pid) in
      Unix.Exit_or_signal.to_string_hum err |> print_endline

let reporter ppf =
  let report _ level ~over k msgf =
    let k _ = over () ; k () in
    let with_time h _ k ppf fmt =
      let time = Time.now () in
      Caml.(Format.kfprintf k ppf ("%a [%s] @[" ^^ fmt ^^ "@]@."))
        Logs.pp_header (level, h) (Time.to_string time)
    in
    msgf @@ fun ?header ?tags fmt -> with_time header tags k ppf fmt
  in
  {Logs.report}

let create_test_db () =
  let ch = Unix.open_process_in "pg_tmp" in
  let url = In_channel.input_all ch in
  Unix.close_process_in ch |> Unix.Exit_or_signal.or_error |> Or_error.ok_exn ;
  Db.create url

let create_db uri =
  try Db.create uri
  with exn ->
    Logs.warn (fun m ->
        m "Connecting to db failed. Cannot run test: %s" (Exn.to_string exn) ) ;
    raise TestDbExn

(** Create a simple database table that only contains integers. *)
let create_simple db name fields values =
  let fields_sql =
    List.map fields ~f:(sprintf "%s integer") |> String.concat ~sep:", "
  in
  Db.(
    exec db (sprintf "create table if not exists %s (%s)" name fields_sql)
    |> command_ok) ;
  List.iter values ~f:(fun vs ->
      let values_sql = List.map vs ~f:Int.to_string |> String.concat ~sep:", " in
      Db.(
        exec db (sprintf "insert into %s values (%s)" name values_sql) |> command_ok)
  )

(** Create a database table. *)
let create db name fields values =
  let fields_sql =
    List.map fields ~f:(fun (f, t) ->
        let open Type.PrimType in
        match t with
        | FixedT _ -> sprintf "%s numeric" f
        | IntT _ -> sprintf "%s integer" f
        | StringT _ -> sprintf "%s varchar" f
        | BoolT _ -> sprintf "%s boolean" f
        | DateT _ -> sprintf "%s date" f
        | _ -> failwith "Unexpected type." )
    |> String.concat ~sep:", "
  in
  Db.(exec db (sprintf "create table %s (%s)" name fields_sql) |> command_ok) ;
  List.iter values ~f:(fun vs ->
      let values_sql = List.map vs ~f:Value.to_sql |> String.concat ~sep:", " in
      Db.(
        exec db (sprintf "insert into %s values (%s)" name values_sql) |> command_ok)
  )

module Expect_test_config = struct
  include Expect_test_config

  let run thunk =
    Logs.set_reporter (reporter Caml.Format.std_formatter) ;
    Logs.set_level (Some Logs.Warning) ;
    try thunk () with TestDbExn -> ()
end

module Test_db = struct
  let conn = create_test_db ()

  let () =
    create_simple conn "r" ["f"; "g"]
      [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]] ;
    create conn "r_date"
      [ ("f", Type.PrimType.DateT {nullable= false})
      ; ("g", Type.PrimType.IntT {nullable= false}) ]
      Date.
        [ [Date (of_string "2018-01-01"); Int 5]
        ; [Date (of_string "2016-12-01"); Int 2]
        ; [Date (of_string "2018-01-23"); Int 3]
        ; [Date (of_string "2017-10-05"); Int 1]
        ; [Date (of_string "2018-01-01"); Int 2]
        ; [Date (of_string "2018-09-01"); Int 4]
        ; [Date (of_string "2018-01-01"); Int 6] ] ;
    create_simple conn "s" ["f"; "g"]
      [[0; 5]; [1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]; [4; 6]] ;
    create_simple conn "log" ["counter"; "succ"; "id"]
      [[1; 4; 1]; [2; 3; 2]; [3; 4; 3]; [4; 6; 1]; [5; 6; 3]] ;
    create_simple conn "r1" ["f"; "g"] [[1; 2]; [1; 3]; [2; 1]; [2; 2]; [3; 4]] ;
    create_simple conn "one" [] [] ;
    create conn "r2"
      [("a", Type.PrimType.FixedT {nullable= false})]
      [ [Fixed (Fixed_point.of_string "0.01")]
      ; [Fixed (Fixed_point.of_string "5")]
      ; [Fixed (Fixed_point.of_string "34.42")]
      ; [Fixed (Fixed_point.of_string "0.88")]
      ; [Fixed (Fixed_point.of_string "-0.42")] ] ;
    create conn "log_str"
      [ ("counter", Type.PrimType.IntT {nullable= false})
      ; ("succ", Type.PrimType.IntT {nullable= false})
      ; ("id", Type.PrimType.StringT {nullable= false}) ]
      [ [Int 1; Int 4; String "foo"]
      ; [Int 2; Int 3; String "fizzbuzz"]
      ; [Int 3; Int 4; String "bar"]
      ; [Int 4; Int 6; String "foo"]
      ; [Int 5; Int 6; String "bar"] ]
end

let make_modules ?layout_file ?(irgen_debug = false) ?(code_only = false) () =
  let module M = Abslayout_db.Make (Test_db) in
  let module S =
    Serialize.Make (struct
        let layout_map_channel = Option.map layout_file ~f:Stdio.Out_channel.create
      end)
      (M)
  in
  let module I =
    Irgen.Make (struct
        let code_only = code_only

        let debug = irgen_debug
      end)
      (M)
      (S)
      ()
  in
  let module C =
    Codegen.Make (struct
        let debug = false
      end)
      (I)
      ()
  in
  ( (module M : Abslayout_db.S)
  , (module S : Serialize.S)
  , (module I : Irgen.S)
  , (module C : Codegen.S) )

module Demomatch = struct
  let example_params =
    [ (Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_p", Value.Int 1)
    ; (Name.create ~type_:Type.PrimType.(IntT {nullable= false}) "id_c", Int 2) ]

  let example_str_params =
    [ ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_p"
      , Value.String "foo" )
    ; ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_c"
      , String "fizzbuzz" ) ]

  let example_db_params =
    [ ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_p"
      , Value.String "-1451410871729396224" )
    ; ( Name.create ~type_:Type.PrimType.(StringT {nullable= false}) "id_c"
      , String "8557539814359574196" ) ]
end
