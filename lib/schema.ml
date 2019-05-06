open Core
open Abslayout0

let scoped s = List.map ~f:(Name.scoped s)

let unscoped = List.map ~f:Name.unscoped

let to_name = function
  | Name n -> Some n
  | As_pred (_, n) -> Some (Name.create n)
  | _ -> None

let rec to_type = function
  | As_pred (p, _) -> to_type p
  | Name n -> Name.Meta.(find_exn n type_)
  | Int _ | Date _
   |Unop ((Year | Month | Day | Strlen | ExtractY | ExtractM | ExtractD), _)
   |Count ->
      IntT {nullable= false}
  | Fixed _ | Avg _ -> FixedT {nullable= false}
  | Bool _ | Exists _
   |Binop ((Eq | Lt | Le | Gt | Ge | And | Or), _, _)
   |Unop (Not, _) ->
      BoolT {nullable= false}
  | String _ -> StringT {nullable= false}
  | Null -> NullT
  | Binop ((Add | Sub | Mul | Div | Mod), p1, p2) ->
      let s1 = to_type p1 in
      let s2 = to_type p2 in
      Type.PrimType.unify s1 s2
  | Binop (Strpos, _, _) -> IntT {nullable= false}
  | Sum p | Min p | Max p -> to_type p
  | If (_, p1, p2) ->
      let s1 = to_type p1 in
      let s2 = to_type p2 in
      Type.PrimType.unify s1 s2
  | First r -> (
    match schema_exn r with
    | [n] -> Name.Meta.(find_exn n type_)
    | [] -> failwith "Unexpected empty schema."
    | _ -> failwith "Too many fields." )
  | Substring _ -> StringT {nullable= false}

and schema_exn r =
  let of_preds =
    List.map ~f:(fun p ->
        let t = to_type p in
        match to_name p with
        | Some n -> Name.copy ~type_:(Some t) n
        | None -> Name.create "__unnamed__" ~type_:t )
  in
  match r.node with
  | AList (_, r) | DepJoin {d_rhs= r; _} -> schema_exn r |> unscoped
  | Select (x, _) | GroupBy (x, _, _) -> of_preds x
  | Filter (_, r) | Dedup r | OrderBy {rel= r; _} -> schema_exn r
  | Join {r1; r2; _} -> schema_exn r1 @ schema_exn r2
  | AOrderedIdx (r1, r2, _) | AHashIdx (r1, r2, _) ->
      (schema_exn r1 |> unscoped) @ schema_exn r2
  | AEmpty -> []
  | AScalar e -> of_preds [e]
  | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema_exn rs
  | ATuple ([], Concat) -> []
  | ATuple (r :: _, Concat) -> schema_exn r
  | As (n, r) -> scoped n (schema_exn r)
  | Relation {r_schema= Some schema; _} -> schema
  | Relation {r_name; r_schema= None; _} ->
      Error.(create "Missing schema annotation." r_name [%sexp_of: string] |> raise)
