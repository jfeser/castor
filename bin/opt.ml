open Core
open Castor
open Collections
open Abslayout_load
module A = Abslayout

let dump fn r =
  Out_channel.with_file fn ~f:(fun ch ->
      Fmt.pf (Format.formatter_of_out_channel ch) "%a" Abslayout.pp r)

(** Run a command and return its output on stdout, logging it if it fails. *)
let command_out cmd =
  let open Or_error.Let_syntax in
  let ch = Core_unix.open_process_in cmd in
  let out = In_channel.input_all ch in
  let%map () =
    Core_unix.Exit_or_signal.or_error (Core_unix.close_process_in ch)
    |> Or_error.tag ~tag:cmd
  in
  out

let system_exn cmd =
  match Core_unix.system cmd with
  | Ok () -> ()
  | Error (`Exit_non_zero code) ->
      failwith @@ sprintf "Command '%s' exited with code %d" cmd code
  | Error (`Signal signal) ->
      failwith
      @@ sprintf "Command '%s' terminated by signal %s" cmd
           (Signal.to_string signal)

let opt conn cost_conn params cost_timeout state query =
  let module Config = struct
    let conn = conn
    let cost_conn = cost_conn
    let params = params
    let cost_timeout = cost_timeout
    let random = state
  end in
  let module T = Transform.Make (Config) in
  match Transform.optimize (module Config) query with
  | First opt_query ->
      if is_ok @@ T.is_serializable opt_query then Some opt_query
      else (
        Logs.warn (fun m -> m "Not serializable:@ %a" A.pp opt_query);
        None)
  | Second failed_subquery ->
      Logs.warn (fun m ->
          m "Optimization failed for subquery:@ %a" A.pp failed_subquery);
      None

let eval dir params query =
  let open Result.Let_syntax in
  Logs.info (fun m -> m "Evaluating:@ %a" A.pp query);

  (* Set up the output directory. *)
  system_exn @@ sprintf "rm -rf %s" dir;
  system_exn @@ sprintf "mkdir -p %s" dir;
  let query_fn = sprintf "%s/query.txt" dir in
  dump query_fn query;

  (* Try to build the query. *)
  let%bind () =
    let compile_cmd =
      let params =
        List.map params ~f:(fun (n, t, _) ->
            Fmt.str "-p %s:%a" n Prim_type.pp t)
        |> String.concat ~sep:" "
      in
      sprintf "castor-compile -o %s %s %s > %s/compile.log 2>&1" dir params
        query_fn dir
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
      sprintf "%s/scanner.exe -t 1 %s/data.bin %s" dir dir params
    in
    let%map out = command_out run_cmd in
    let time, _ = String.lsplit2_exn ~on:' ' out in
    String.rstrip ~drop:Char.is_alpha time |> Float.of_string
  in

  run_time

let trial_dir = sprintf "%s-trial"

let copy_out out_file out_dir query =
  dump out_file query;
  system_exn @@ sprintf "rm -rf %s" out_dir;
  system_exn @@ sprintf "mv -f %s %s" (trial_dir out_dir) out_dir

let main ~params ~cost_timeout ~timeout ~out_dir ~out_file ch =
  Random.init 0;

  let conn = Db.create (Sys.getenv_exn "CASTOR_OPT_DB") in
  let cost_conn = conn in
  let params_set =
    List.map params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let query =
    load_string_exn ~params:params_set conn @@ In_channel.input_all ch
  in

  let best_cost = ref Float.infinity in
  let cost state =
    Fresh.reset Global.fresh;
    match opt conn cost_conn params_set cost_timeout state query with
    | Some query' -> (
        match eval (trial_dir out_dir) params query' with
        | Ok cost ->
            if Float.(cost < !best_cost) then (
              copy_out out_file out_dir query';
              best_cost := cost);
            cost
        | Error err ->
            Logs.warn (fun m -> m "Evaluation failed: %a" Error.pp err);
            Float.infinity)
    | None -> Float.infinity
  in

  let cost = Memo.of_comparable (module Mcmc.Random_choice.C) cost in
  let max_time = Option.map ~f:Time.Span.of_sec timeout in

  try Mcmc.run ?max_time cost |> ignore
  with Resolve.Resolve_error r -> Fmt.epr "%a@." (Resolve.pp_err Fmt.nop) r

let spec =
  let open Command.Let_syntax in
  [%map_open
    let () = Log.param
    and () = Ops.param
    and () = Db.param
    and () = Type_cost.param
    and () = Join_opt.param
    and () = Groupby_tactics.param
    and () = Type.param
    and () = Simplify_tactic.param
    and cost_timeout =
      flag "cost-timeout" (optional float)
        ~doc:"SEC time to run cost estimation"
    and timeout =
      flag "timeout" (optional float) ~doc:"SEC time to run optimizer"
    and params =
      flag "param" ~aliases:[ "p" ]
        (listed Util.param_and_value)
        ~doc:"NAME:TYPE query parameters"
    and out_dir =
      flag "out-dir" (required string) ~aliases:[ "o" ]
        ~doc:"DIR output directory"
    and out_file =
      flag "out-file" (required string) ~aliases:[ "f" ]
        ~doc:"FILE output directory"
    and ch =
      anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
    in
    fun () -> main ~params ~cost_timeout ~timeout ~out_dir ~out_file ch]

let () = Command.basic spec ~summary:"Optimize a query." |> Command_unix.run
