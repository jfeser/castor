open! Core

module PrimType = struct
  type t =
    | NullT
    | IntT of {nullable: bool}
    | DateT of {nullable: bool}
    | FixedT of {nullable: bool}
    | StringT of {nullable: bool; padded: bool}
    | BoolT of {nullable: bool}
    | TupleT of t list
    | VoidT
  [@@deriving compare, hash, sexp]

  let null_t = NullT

  let int_t = IntT {nullable= false}

  let date_t = DateT {nullable= false}

  let fixed_t = FixedT {nullable= false}

  let string_t = StringT {nullable= false; padded= false}

  let bool_t = BoolT {nullable= false}

  let of_primvalue = function
    | `Int _ -> IntT {nullable= false}
    | `String _ -> StringT {nullable= false; padded= false}
    | `Bool _ -> BoolT {nullable= false}
    | `Null -> NullT

  let to_sql = function
    | BoolT _ -> "boolean"
    | IntT _ -> "integer"
    | FixedT _ -> "numeric"
    | StringT _ -> "varchar"
    | DateT _ -> "date"
    | TupleT _ | VoidT | NullT -> failwith "Not a value type."

  let to_string : t -> string = function
    | BoolT _ -> "bool"
    | IntT _ -> "int"
    | FixedT _ -> "fixed"
    | StringT _ -> "string"
    | NullT -> "null"
    | VoidT -> "void"
    | TupleT _ -> "tuple"
    | DateT _ -> "date"

  let rec pp_tuple pp_v fmt =
    let open Format in
    function
    | [] -> fprintf fmt ""
    | [x] -> fprintf fmt "%a" pp_v x
    | x :: xs -> fprintf fmt "%a,@ %a" pp_v x (pp_tuple pp_v) xs

  let rec pp =
    let open Format in
    fun fmt -> function
      | NullT -> fprintf fmt "Null"
      | IntT {nullable= true} -> fprintf fmt "Int"
      | IntT {nullable= false} -> fprintf fmt "Int[nonnull]"
      | StringT {nullable= true; _} -> fprintf fmt "String"
      | StringT {nullable= false; _} -> fprintf fmt "String[nonnull]"
      | BoolT {nullable= true} -> fprintf fmt "Bool"
      | BoolT {nullable= false} -> fprintf fmt "Bool[nonnull]"
      | TupleT ts -> fprintf fmt "Tuple[%a]" (pp_tuple pp) ts
      | VoidT -> fprintf fmt "Void"
      | FixedT {nullable= true} -> fprintf fmt "Fixed"
      | FixedT {nullable= false} -> fprintf fmt "Fixed[nonnull]"
      | DateT {nullable= true} -> fprintf fmt "Date"
      | DateT {nullable= false} -> fprintf fmt "Date[nonnull]"

  let is_nullable = function
    | NullT -> true
    | IntT {nullable= n}
     |BoolT {nullable= n}
     |StringT {nullable= n; _}
     |FixedT {nullable= n}
     |DateT {nullable= n} ->
        n
    | TupleT _ | VoidT -> false

  let rec unify t1 t2 =
    match (t1, t2) with
    | IntT {nullable= n1}, IntT {nullable= n2} -> IntT {nullable= n1 || n2}
    | DateT {nullable= n1}, DateT {nullable= n2} -> DateT {nullable= n1 || n2}
    | FixedT {nullable= n1}, FixedT {nullable= n2} -> FixedT {nullable= n1 || n2}
    (* TODO: Remove coercion *)
    | FixedT {nullable= n1}, IntT {nullable= n2}
     |IntT {nullable= n1}, FixedT {nullable= n2} ->
        FixedT {nullable= n1 || n2}
    | BoolT {nullable= n1}, BoolT {nullable= n2} -> BoolT {nullable= n1 || n2}
    | StringT {nullable= n1; _}, StringT {nullable= n2; _} ->
        StringT {nullable= n1 || n2; padded= false}
    | TupleT t1, TupleT t2 -> TupleT (List.map2_exn t1 t2 ~f:unify)
    | VoidT, VoidT -> VoidT
    | NullT, _
     |IntT _, _
     |DateT _, _
     |FixedT _, _
     |StringT _, _
     |BoolT _, _
     |TupleT _, _
     |VoidT, _ ->
        Error.create "Nonunifiable." (t1, t2) [%sexp_of: t * t] |> Error.raise

  let unify t1 t2 =
    Or_error.try_with (fun () -> unify t1 t2)
    |> (fun err ->
         Or_error.tag_arg err "Failed to unify." (t1, t2) [%sexp_of: t * t] )
    |> Or_error.ok_exn

  let rec width = function
    | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> 1
    | TupleT ts -> List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
    | VoidT -> 0
end
