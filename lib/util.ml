open Core

(** Run a command, logging it if it fails. *)
let command_exn ?quiet:_ = function
  | [] -> Error.of_string "Empty command" |> Error.raise
  | cmd ->
      let cmd_str = String.concat cmd ~sep:" " in
      Log.info (fun m -> m "%s" cmd_str);
      let err = Core_unix.system cmd_str in
      Or_error.(
        tag_arg
          (Core_unix.Exit_or_signal.or_error err)
          "Running command failed." cmd_str [%sexp_of: string]
        |> ok_exn)

(** Run a command and return its output on stdout, logging it if it fails. *)
let command_out_exn ?quiet:_ = function
  | [] -> Error.of_string "Empty command" |> Error.raise
  | cmd ->
      let open Core_unix in
      let cmd_str = String.concat cmd ~sep:" " in
      Log.info (fun m -> m "%s" cmd_str);
      let ch = open_process_in cmd_str in
      let out = In_channel.input_all ch in
      Or_error.(
        tag_arg
          (Exit_or_signal.or_error (close_process_in ch))
          "Running command failed." cmd_str [%sexp_of: string]
        |> ok_exn);
      out

let param_of_string s =
  let lexbuf = Lexing.from_string s in
  try Ralgebra_parser.param_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    let msg = sprintf "Parse error: %s (line: %d, col: %d)" msg line col in
    Exn.reraise e msg

let param =
  let open Command in
  Arg_type.create (fun s ->
      let name, type_, _ = param_of_string s in
      (name, type_))

let param_and_value =
  let open Command in
  Arg_type.create (fun s ->
      let name, type_, m_value = param_of_string s in
      let v =
        Option.value_exn m_value ~error:(Error.of_string "Expected a value.")
      in
      let v = Value.of_pred v in
      (name, type_, v))

let channel =
  let open Command in
  Arg_type.create In_channel.create

class ['s] tuple2_monoid m1 m2 =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = (m1#zero, m2#zero)
    method private plus (x, y) (x', y') = (m1#plus x x', m2#plus y y')
  end

class ['s] list_monoid =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = []
    method private plus = ( @ )
  end

class ['s] set_monoid m =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = Set.empty m
    method private plus = Set.union
  end

class ['s] map_monoid m =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = Map.empty m

    method private plus =
      Map.merge ~f:(fun ~key:_ -> function
        | `Both _ -> failwith "Duplicate key" | `Left x | `Right x -> Some x)
  end

class ['s] conj_monoid =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = true
    method private plus = ( && )
  end

class ['s] disj_monoid =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = false
    method private plus = ( || )
  end

class ['s] int_sum_monoid =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = 0
    method private plus = ( + )
  end

class ['s] float_sum_monoid =
  object
    inherit ['s] VisitorsRuntime.monoid
    method private zero = 0.0
    method private plus = ( +. )
  end

let run_in_fork (type a) (thunk : unit -> a) : a =
  let rd, wr = Core_unix.pipe () in
  let rd = Core_unix.in_channel_of_descr rd in
  let wr = Core_unix.out_channel_of_descr wr in
  match Core_unix.fork () with
  | `In_the_child ->
      Marshal.(to_channel wr (thunk ()) [ Closures ]);
      exit 0
  | `In_the_parent _ -> Marshal.(from_channel rd)

let run_in_fork_timed (type a) ?time ?(sleep_sec = 0.001) (thunk : unit -> a) :
    a option =
  let rd, wr = Core_unix.pipe () in
  let rd = Core_unix.in_channel_of_descr rd in
  let wr = Core_unix.out_channel_of_descr wr in
  match Core_unix.fork () with
  | `In_the_child ->
      Marshal.(to_channel wr (thunk ()) [ Closures ]);
      exit 0
  | `In_the_parent pid -> (
      match time with
      | Some span ->
          let start = Time.now () in
          let rec sleep () =
            if Time.Span.(Time.(diff (now ()) start) > span) then (
              Signal_unix.send_i Signal.kill (`Pid pid);
              None)
            else (
              Core_unix.nanosleep sleep_sec |> ignore;
              match Core_unix.wait_nohang (`Pid pid) with
              | None -> sleep ()
              | Some _ -> Some Marshal.(from_channel rd))
          in
          sleep ()
      | None -> Some Marshal.(from_channel rd))
