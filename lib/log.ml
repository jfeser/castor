let src = Logs.Src.create ~doc:"Main logging source for Castor." "castor"

let setup_log level =
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
          level Time.pp (Time.now ()) (Src.name src)
      in
      msgf format
    in
    { report }
  in
  let ppf = Fmt_tty.setup Out_channel.stderr in
  Format.pp_set_margin ppf 120;
  Logs.set_level (Some level);
  Logs.set_reporter (format_reporter ppf)

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
