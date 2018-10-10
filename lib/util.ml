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

let param =
  let open Command in
  Arg_type.create (fun s ->
      let k, v = String.lsplit2_exn ~on:':' s in
      let v =
        let open Type.PrimType in
        match v with
        | "string" -> StringT {nullable= false}
        | "int" -> IntT {nullable= false}
        | "bool" -> BoolT {nullable= false}
        | "date" -> IntT {nullable= false}
        | "float" -> FixedT {nullable= false}
        | _ -> failwith "Unexpected type name."
      in
      (k, v) )

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
