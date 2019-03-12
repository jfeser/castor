open Core

let command_exn ?quiet:_ = function
  | [] -> Error.of_string "Empty command" |> Error.raise
  | cmd ->
      let cmd_str = String.concat cmd ~sep:" " in
      Logs.info (fun m -> m "%s" cmd_str) ;
      let err = Unix.system cmd_str in
      err |> Unix.Exit_or_signal.or_error
      |> fun err ->
      Or_error.tag_arg err "Running command failed." cmd_str [%sexp_of: string]
      |> Or_error.ok_exn

let command_out_exn ?quiet:_ = function
  | [] -> Error.of_string "Empty command" |> Error.raise
  | cmd ->
      let cmd_str = String.concat cmd ~sep:" " in
      Logs.info (fun m -> m "%s" cmd_str) ;
      let err = Unix.system cmd_str in
      err |> Unix.Exit_or_signal.or_error
      |> fun err ->
      Or_error.tag_arg err "Running command failed." cmd_str [%sexp_of: string]
      |> Or_error.ok_exn

let param_of_string s =
  let lexbuf = Lexing.from_string s in
  try Ralgebra_parser.param_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let param =
  let open Command in
  Arg_type.create (fun s ->
      let name, type_, _ = param_of_string s in
      (name, type_) )

let param_and_value =
  let open Command in
  Arg_type.create (fun s ->
      let name, type_, m_value = param_of_string s in
      let v =
        Option.value_exn m_value ~error:(Error.of_string "Expected a value.")
      in
      let v = Value.of_pred v in
      (name, type_, v) )

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
        | `Both _ -> failwith "Duplicate key" | `Left x | `Right x -> Some x )
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
