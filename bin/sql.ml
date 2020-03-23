open! Core
open Castor
open Collections
open Abslayout_load
module A = Abslayout

let main ~params:all_params ~simplify ~project ~unnest ~sql ~cse ch =
  Logs.set_level (Some Debug);
  Logs.Src.set_level Log.src (Some Debug);
  Format.set_margin 120;

  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let module Config = struct
    let conn = Db.create (Sys.getenv_exn "CASTOR_DB")

    let params = params
  end in
  let module S = Simplify_tactic.Make (Config) in
  let module O = Ops.Make (Config) in
  let simplify q =
    if simplify then
      let q =
        Cardinality.annotate ~dedup:true q
        |> Join_elim.remove_joins |> Unnest.hoist_meta |> A.strip_meta
      in
      Option.value_exn (O.apply S.simplify Path.root q)
    else q
  in

  let query_str = In_channel.input_all ch in
  let query = load_string ~params Config.conn query_str in
  let query = simplify query in
  let query =
    let q =
      if unnest then Unnest.unnest query
      else
        ( Cardinality.annotate query
          :> < cardinality_matters : bool ; why_card_matters : string >
             Ast.annot )
    in
    Cardinality.extend ~dedup:true q
    |> Join_elim.remove_joins |> Unnest.hoist_meta |> A.strip_meta
  in
  let query = simplify query in
  let query = if project then Project.project query else query in
  let query = simplify query in
  let query =
    if cse then `Sql (Cse.extract_common query |> Cse.to_sql) else `Query query
  in
  let query =
    if sql then
      match query with
      | `Sql _ -> query
      | `Query q -> `Sql (Sql.of_ralgebra q |> Sql.to_string)
    else query
  in
  match query with
  | `Sql x -> Sql.format x |> print_endline
  | `Query x -> Format.printf "%a@." Abslayout.pp x

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
      and project =
        flag "project" ~aliases:[ "r" ] no_arg ~doc:"project the query"
      and cse = flag "cse" ~aliases:[ "c" ] no_arg ~doc:"apply cse"
      and params =
        flag "param" ~aliases:[ "p" ]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~params ~simplify ~project ~unnest ~sql ~cse ch]
  |> Command.run
