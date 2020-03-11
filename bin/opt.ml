open! Core
open Castor
open Collections
open Castor_opt
open Abslayout_load

let main ~params:all_params ~cost_timeout ch =
  Logs.set_level (Some Info);
  Logs.info (fun m ->
      m "%s" (Sys.get_argv () |> Array.to_list |> String.concat ~sep:" "));
  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let conn = Db.create (Sys.getenv_exn "CASTOR_DB") in
  let module Config = struct
    let conn = conn

    let cost_conn =
      match Sys.getenv "CASTOR_COST_DB" with
      | Some db -> Db.create db
      | None ->
          Logs.warn (fun m -> m "Using main db to estimate costs.");
          conn

    let params = params

    let cost_timeout = cost_timeout
  end in
  let module T = Transform.Make (Config) in
  let module O = Ops.Make (Config) in
  let query_str = In_channel.input_all ch in
  let query = load_string ~params conn query_str in
  match Transform.optimize (module Config) query with
  | Some query' ->
      Or_error.iter_error (T.is_serializable query') ~f:(fun err ->
          Logs.warn (fun m -> m "Query is not serializable: %a" Error.pp err));
      Format.printf "%a" Abslayout.pp query'
  | None -> Logs.warn (fun m -> m "Optimization failed.")

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Optimize a query."
    [%map_open
      let () = Log.param
      and cost_timeout =
        flag "cost-timeout" (optional float)
          ~doc:"terminate cost function after n seconds"
      and params =
        flag "param" ~aliases:[ "p" ]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~params ~cost_timeout ch]
  |> Command.run
