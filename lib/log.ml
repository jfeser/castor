open Core

let src = Logs.Src.create ~doc:"Main logging source for Castor." "castor"

let format_reporter ppf =
  let open Logs in
  let report src level ~over k msgf =
    let style =
      match level with
      | Logs.App -> `White
      | Debug -> `Green
      | Info -> `Blue
      | Warning -> `Yellow
      | Error -> `Red
    in
    let k _ =
      over ();
      k ()
    in
    let format ?header:_ ?tags:_ fmt =
      Fmt.kpf k ppf
        ("@[[%a] [%a] [%s]@ " ^^ fmt ^^ "@]@.")
        Fmt.(styled style Logs.pp_level)
        level Time_unix.pp (Time.now ()) (Src.name src)
    in
    msgf format
  in
  { report }

let setup_stderr () =
  let ppf = Fmt_tty.setup Out_channel.stderr in
  Format.pp_set_margin ppf 120;
  Logs.set_reporter (format_reporter ppf)

let setup_log level =
  setup_stderr ();
  Logs.Src.set_level src (Some level)

let level_type =
  let levels = [ "debug"; "info"; "warning"; "error" ] in
  Command.Arg_type.create
    ~complete:(fun _ ~part ->
      List.filter levels ~f:(String.is_prefix ~prefix:part))
    (fun l ->
      match Logs.level_of_string l with
      | Ok l -> l
      | Error (`Msg m) -> failwith m)

let param =
  let open Command.Let_syntax in
  [%map_open
    let verbose =
      flag "verbose" ~aliases:[ "v" ] no_arg ~doc:"increase verbosity"
    and quiet =
      flag "quiet" ~aliases:[ "q" ] no_arg ~doc:"decrease verbosity"
    in
    let level =
      if verbose then Logs.Debug else if quiet then Logs.Error else Logs.Info
    in
    setup_log level]

include (val Logs.src_log src : Logs.LOG)

let with_level src level f =
  let old_level = Logs.Src.level src in
  let old_reporter = Logs.reporter () in
  protect
    ~f:(fun () ->
      Logs.Src.set_level src old_level;
      let ppf = Fmt_tty.setup Out_channel.stderr in
      Format.pp_set_margin ppf 120;
      Logs.Src.set_level src (Some level);
      Logs.set_reporter (format_reporter ppf);
      f ())
    ~finally:(fun () ->
      Logs.set_reporter old_reporter;
      Logs.Src.set_level src old_level)

module type LOG = sig
  include Logs.LOG

  val src : Logs.Src.t
  val param : unit Command.Param.t
  val info_s : (unit -> Sexp.t) -> unit
end

let make ?(level = Some Logs.Info) name =
  (module struct
    let src =
      let s = Logs.Src.create name in
      Logs.Src.set_level s level;
      s

    let param =
      let open Command.Let_syntax in
      [%map_open
        let level =
          flag
            (sprintf "set-log-level-%s" name)
            (optional level_type) ~doc:"LEVEL set verbosity"
        in
        Option.iter level ~f:(Logs.Src.set_level src)]

    include (val Logs.src_log src)

    let info_s f = info (fun m -> m "%a" Sexp.pp_hum (f ()))
  end : LOG)
