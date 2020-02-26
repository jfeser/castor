open! Core
open Castor
open Collections
open Castor_opt
open Abslayout_load

let main ~params:all_params ~simplify ~unnest ~sql ch =
  Logs.set_level (Some Debug);
  Logs.Src.set_level Log.src (Some Debug);
  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let param_ctx =
    List.map all_params ~f:(fun (n, t, v) -> (Name.create ~type_:t n, v))
    |> Map.of_alist_exn (module Name)
  in
  let module Config = struct
    let conn = Db.create (Sys.getenv_exn "CASTOR_DB")

    let params = params

    let param_ctx = param_ctx

    let validate = false
  end in
  let module S = Simplify_tactic.Make (Config) in
  let module O = Ops.Make (Config) in
  let query_str = In_channel.input_all ch in
  let query = load_string ~params Config.conn query_str in
  let query = if unnest then Unnest.unnest query else query in
  let query =
    if simplify then Option.value_exn (O.apply S.simplify Path.root query)
    else query
  in
  if sql then Sql.of_ralgebra query |> Sql.to_string_hum |> print_endline
  else Format.printf "%a@." Abslayout.pp query

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Optimize a query for sql."
    [%map_open
      let () = Log.param
      and sql = flag "sql" no_arg ~doc:"dump sql"
      and simplify =
        flag "simplify" ~aliases:[ "s" ] no_arg ~doc:"simplify the query"
      and unnest =
        flag "unnest" ~aliases:[ "u" ] no_arg ~doc:"unnest before simplifying"
      and params =
        flag "param" ~aliases:[ "p" ]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~params ~simplify ~unnest ~sql ch]
  |> Command.run
