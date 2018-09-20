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
        | _ -> failwith "Unexpected type name."
      in
      (k, v) )
