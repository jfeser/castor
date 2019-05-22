open! Core
open Castor
open Collections
open Castor_opt

let main ~params:all_params ~db ~validate ch =
  Logs.info (fun m ->
      m "%s" (Sys.argv |> Array.to_list |> String.concat ~sep:" ") ) ;
  let params =
    List.map all_params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let param_ctx =
    List.map all_params ~f:(fun (n, t, v) -> (Name.create ~type_:t n, v))
    |> Map.of_alist_exn (module Name)
  in
  let module Config = struct
    let conn = db

    let dbconn = Db.conn db

    let params = params

    let param_ctx = param_ctx

    let validate = validate
  end in
  let module A = Abslayout_db.Make (Config) in
  let module T = Transform.Make (Config) () in
  let module O = Ops.Make (Config) in
  let query_str = In_channel.input_all ch in
  let query = A.load_string ~params query_str in
  match Transform.optimize (module Config) query with
  | Some query' ->
      Or_error.iter_error (T.is_serializable query') ~f:(fun err ->
          Logs.warn (fun m -> m "Query is not serializable: %a" Error.pp err)
      ) ;
      Format.printf "%a" Abslayout.pp query'
  | None -> ()

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Optimize a query."
    [%map_open
      let () = Log.param
      and validate =
        flag "validate" ~aliases:["c"] no_arg ~doc:"validate transforms"
      and db = Db.param
      and params =
        flag "param" ~aliases:["p"]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~params ~db ~validate ch]
  |> Command.run
