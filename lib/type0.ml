open Base
module Format = Caml.Format

module PrimType = struct
  type t =
    | NullT
    | IntT of {nullable: bool}
    | StringT of {nullable: bool}
    | BoolT of {nullable: bool}
    | TupleT of t list
    | VoidT
  [@@deriving compare, hash, sexp]

  let of_primvalue = function
    | `Int _ -> IntT {nullable= false}
    | `String _ -> StringT {nullable= false}
    | `Bool _ -> BoolT {nullable= false}
    | `Unknown _ -> StringT {nullable= false}
    | _ -> failwith "Unknown type."

  let to_string : t -> string = function
    | BoolT _ -> "bool"
    | IntT _ -> "int"
    | StringT _ -> "string"
    | NullT -> "null"
    | VoidT -> "void"
    | TupleT _ -> "tuple"

  let of_dtype = function
    | Db.DInt -> IntT {nullable= false}
    | Db.DString -> StringT {nullable= false}
    | Db.DBool -> BoolT {nullable= false}
    | Db.DRational -> StringT {nullable= false}
    | t -> Error.create "Unexpected dtype." t [%sexp_of: Db.dtype] |> Error.raise

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
      | StringT {nullable= true} -> fprintf fmt "String"
      | StringT {nullable= false} -> fprintf fmt "String[nonnull]"
      | BoolT {nullable= true} -> fprintf fmt "Bool"
      | BoolT {nullable= false} -> fprintf fmt "Bool[nonnull]"
      | TupleT ts -> fprintf fmt "Tuple[%a]" (pp_tuple pp) ts
      | VoidT -> fprintf fmt "Void"

  let is_nullable = function
    | NullT -> true
    | IntT {nullable= n} | BoolT {nullable= n} | StringT {nullable= n} -> n
    | TupleT _ | VoidT -> false

  let rec unify t1 t2 =
    match (t1, t2) with
    | IntT {nullable= n1}, IntT {nullable= n2} -> IntT {nullable= n1 || n2}
    | BoolT {nullable= n1}, BoolT {nullable= n2} -> BoolT {nullable= n1 || n2}
    | StringT {nullable= n1}, StringT {nullable= n2} -> StringT {nullable= n1 || n2}
    | TupleT t1, TupleT t2 -> TupleT (List.map2_exn t1 t2 ~f:unify)
    | VoidT, VoidT -> VoidT
    | _, _ -> Error.create "Nonunifiable." (t1, t2) [%sexp_of: t * t] |> Error.raise

  let rec width = function
    | NullT | IntT _ | BoolT _ | StringT _ -> 1
    | TupleT ts -> List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
    | VoidT -> 0
end
