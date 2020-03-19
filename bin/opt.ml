open! Core
open Castor
open Collections
open Castor_opt
open Abslayout_load

(** Run a command and return its output on stdout, logging it if it fails. *)
let command_out cmd =
  let open Or_error.Let_syntax in
  let ch = Unix.open_process_in cmd in
  let out = In_channel.input_all ch in
  let%map () = Unix.Exit_or_signal.or_error (Unix.close_process_in ch) in
  out

let system_exn cmd =
  match Unix.system cmd with
  | Ok () -> ()
  | Error (`Exit_non_zero code) ->
      failwith @@ sprintf "Command '%s' exited with code %d" cmd code
  | Error (`Signal signal) ->
      failwith
      @@ sprintf "Command '%s' terminated by signal %s" cmd
           (Signal.to_string signal)

let opt conn cost_conn params cost_timeout state query =
  let open Option.Let_syntax in
  let module Config = struct
    let conn = conn

    let cost_conn = cost_conn

    let params = params

    let cost_timeout = cost_timeout

    let random = state
  end in
  let module T = Transform.Make (Config) in
  let%map query' = Transform.optimize (module Config) query in
  (query', Or_error.is_ok @@ T.is_serializable query')

let eval out_dir params query =
  let open Result.Let_syntax in
  (* Set up the output directory. *)
  system_exn @@ sprintf "rm -rf %s" out_dir;
  system_exn @@ sprintf "mkdir -p %s" out_dir;
  let query_fn = sprintf "%s/query.txt" out_dir in
  Out_channel.with_file query_fn ~f:(fun ch ->
      Format.fprintf
        (Format.formatter_of_out_channel ch)
        "%a" Abslayout.pp query);

  (* Try to build the query. *)
  let%bind () =
    let compile_cmd =
      let params =
        List.map params ~f:(fun (n, t, _) ->
            Fmt.str "-p %s:%a" n Prim_type.pp t)
        |> String.concat ~sep:" "
      in
      sprintf
        "$CASTOR_ROOT/../_build/default/castor/bin/compile.exe -o %s %s %s > \
         %s/compile.log 2>&1"
        out_dir params query_fn out_dir
    in
    let%map out = command_out compile_cmd in
    Logs.info (fun m -> m "Compile output: %s" out)
  in

  (* Try to run the query. *)
  let%map run_time =
    let run_cmd =
      let params =
        List.map params ~f:(fun (_, _, v) -> sprintf "'%s'" @@ Value.to_param v)
        |> String.concat ~sep:" "
      in
      sprintf "%s/scanner.exe -t 1 %s/data.bin %s" out_dir out_dir params
    in
    let%map out = command_out run_cmd in
    let time, _ = String.lsplit2_exn ~on:' ' out in
    String.rstrip ~drop:Char.is_alpha time |> Float.of_string
  in

  run_time

let main ~params ~cost_timeout ~timeout ~out_dir ch =
  (* Set up logging. *)
  Logs.set_level (Some Info);
  Logs.Src.set_level Db.src (Some Warning);
  Logs.Src.set_level Ops.src (Some Warning);
  Logs.Src.set_level Type_cost.src (Some Warning);
  Logs.Src.set_level Join_opt.src (Some Warning);
  Logs.info (fun m ->
      m "%s" (Sys.get_argv () |> Array.to_list |> String.concat ~sep:" "));

  let conn = Db.create (Sys.getenv_exn "CASTOR_DB")
  and cost_conn = Db.create (Sys.getenv_exn "CASTOR_COST_DB") in
  let params_set =
    List.map params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let query = load_string ~params:params_set conn @@ In_channel.input_all ch in

  let cost state =
    Fresh.reset Global.fresh;
    match opt conn cost_conn params_set cost_timeout state query with
    | Some (query', serializable) when serializable -> (
        match eval out_dir params query' with
        | Ok cost -> cost
        | _ -> Float.infinity )
    | _ -> Float.infinity
  in
  let cost = Memo.of_comparable (module Mcmc.Random_choice.C) cost in
  let max_time = Option.map ~f:Time.Span.of_sec timeout in
  Mcmc.run ?max_time cost |> ignore

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Optimize a query."
    [%map_open
      let () = Log.param
      and cost_timeout =
        flag "cost-timeout" (optional float)
          ~doc:"terminate cost function after n seconds"
      and timeout =
        flag "timeout" (optional float)
          ~doc:"terminate optimization after n seconds"
      and params =
        flag "param" ~aliases:[ "p" ]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and out_dir =
        flag "out-dir" (required string) ~aliases:[ "o" ]
          ~doc:"output directory"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~params ~cost_timeout ~timeout ~out_dir ch]
  |> Command.run
