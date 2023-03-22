open Core
open Sql2code

let () =
  let input_str = In_channel.input_all In_channel.stdin in
  let queries = load_queries input_str in
  let db_schema = Db_schema.of_queries queries in
  let tables =
    Map.keys db_schema.tables |> List.sort ~compare:[%compare: string]
  in
  let dataclasses = List.map tables ~f:(emit_dataclass db_schema) in
  let add_methods = List.map tables ~f:(emit_add db_schema) in
  let remove_methods = List.map tables ~f:(emit_remove db_schema) in
  let query_methods =
    List.filter_map queries ~f:(function
      | Select s, pos -> Some (s, pos)
      | _ -> None)
    |> List.filter_mapi ~f:(fun i (select, pos) ->
           let query_str = String.strip @@ slice_pos input_str pos in
           try
             Some
               (emit_select db_schema query_str (Fmt.str "query_%d" i) select)
           with exn ->
             Fmt.epr "@[<v>could not emit plan:@ %a@]@." Exn.pp exn;
             Backtrace.Exn.most_recent_for_exn exn
             |> Option.iter ~f:(fun bt ->
                    Fmt.epr "%s\n" (Backtrace.to_string bt));
             None)
  in
  let indexes =
    db_schema.indexes
    |> List.filter ~f:(fun (_, _, _, used) -> !used)
    |> List.sort ~compare:(fun (_, _, t, _) (_, _, t', _) ->
           [%compare: string] t t')
  in
  let index_inits = List.map indexes ~f:emit_index_init in
  let index_types = List.map indexes ~f:(emit_index_type db_schema) in
  eprint_s [%message (db_schema : Db_schema.t)];
  let hints =
    let table_types =
      Map.keys db_schema.tables
      |> List.map ~f:(fun table_name ->
             Fmt.str "%s : list[%s]" table_name (dataclass_name table_name))
    in
    Python_block.Seq (Stmts table_types :: index_types)
  in
  let init =
    Python_block.Block
      {
        header = "def __init__(self) -> None:";
        body =
          Seq
            ([
               Python_block.Stmts
                 (Map.keys db_schema.tables
                 |> List.map ~f:(Fmt.str "self.%s = []"));
             ]
            @ index_inits);
      }
  in
  let app =
    Python_block.Seq
      [
        Stmts
          [
            "from collections import defaultdict";
            "from dataclasses import dataclass, field";
            "import datetime";
            "import decimal";
          ];
        Seq dataclasses;
        Block
          {
            header = "class App:";
            body =
              Seq
                [
                  hints;
                  init;
                  Seq add_methods;
                  Seq remove_methods;
                  Seq query_methods;
                ];
          };
      ]
  in
  Python_block.to_string app |> print_endline
