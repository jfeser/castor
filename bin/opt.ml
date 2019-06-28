open! Core
open Castor
open Collections
open Castor_opt

let main ~params:all_params ~db ~cost_db ~validate ~cost_timeout ch =
  Logs.Src.set_level Log.src (Some Debug) ;
  Logs.info (fun m ->
      m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ")) ;
  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let param_ctx =
    List.map all_params ~f:(fun (n, t, v) -> (Name.create ~type_:t n, v))
    |> Map.of_alist_exn (module Name)
  in
  let module Config = struct
    let conn = Db.create db

    let cost_conn =
      Option.map cost_db ~f:Db.create |> Option.value ~default:conn

    let params = params

    let param_ctx = param_ctx

    let validate = validate

    let simplify = None

    let cost_timeout = cost_timeout
  end in
  let module A = Abslayout_db.Make (Config) in
  let module T = Transform.Make (Config) () in
  let module O = Ops.Make (Config) in
  let query_str = In_channel.input_all ch in
  let query = A.load_string ~params query_str in
  match Transform.optimize (module Config) query with
  | Some query' ->
      Or_error.iter_error (T.is_serializable query') ~f:(fun err ->
          Logs.warn (fun m -> m "Query is not serializable: %a" Error.pp err)) ;
      Format.printf "%a" Abslayout.pp query'
  | None -> Logs.warn (fun m -> m "Optimization failed.")

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Optimize a query."
    [%map_open
      let () = Log.param
      and validate =
        flag "validate" ~aliases:["c"] no_arg ~doc:"validate transforms"
      and db = flag "db" (required string) ~doc:"CONNINFO the main database"
      and cost_db =
        flag "cost-db" (optional string)
          ~doc:"CONNINFO the database to use for computing costs"
      and cost_timeout =
        flag "cost-timeout" (optional float)
          ~doc:"terminate cost function after n seconds"
      and params =
        flag "param" ~aliases:["p"]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~params ~db ~cost_db ~cost_timeout ~validate ch]
  |> Command.run
