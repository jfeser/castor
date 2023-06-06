open Core
open Ast
open Prim_type
module V = Visitors

type t = Name.t list [@@deriving compare, equal, sexp]

type opt_t = (string option * Prim_type.t option) list
[@@deriving compare, sexp]

let pp = Fmt.Dump.list Name.pp

let to_type_open schema to_type = function
  | `Sum p | `Min p | `Max p -> to_type p
  | `Name n -> Name.type_exn n
  | `Date _ | `Unop ((Unop.Year | Month | Day), _) -> date_t
  | `Int _ | `Row_number
  | `Unop ((Strlen | ExtractY | ExtractM | ExtractD), _)
  | `Count
  | `Binop (Binop.Strpos, _, _) ->
      int_t
  | `Fixed _ | `Avg _ -> fixed_t
  | `Bool _ | `Exists _
  | `Binop ((Eq | Lt | Le | Gt | Ge | And | Or), _, _)
  | `Unop (Not, _) ->
      bool_t
  | `String _ | `Substring _ -> string_t
  | `Null None -> failwith "Untyped null."
  | `Null (Some t) -> t
  | `Binop ((Add | Sub | Mul | Div | Mod), p1, p2) | `If (_, p1, p2) ->
      unify (to_type p1) (to_type p2)
  | `First r -> (
      match schema r with
      | [ n ] -> Name.type_exn n
      | [] -> failwith "Unexpected empty schema."
      | _ -> failwith "Too many fields.")

let schema_query_open schema r =
  let rec to_type p = to_type_open schema to_type p in
  let of_preds =
    List.map ~f:(fun (p, n) ->
        let t = Or_error.try_with (fun () -> to_type p) |> Or_error.ok in
        Name.create ?type_:t n)
  in
  let schema =
    match r with
    | AList { l_values = r; _ }
    | DepJoin { d_rhs = r; _ }
    | Filter (_, r)
    | Dedup r
    | OrderBy { rel = r; _ }
    | Limit (_, r) ->
        schema r
    | AOrderedIdx { oi_keys = lhs; oi_values = rhs; _ }
    | AHashIdx { hi_keys = lhs; hi_values = rhs; _ } ->
        schema lhs @ schema rhs
    | Select (x, _) | GroupBy (x, _, _) -> Select_list.to_list x |> of_preds
    | Join { r1; r2; _ } -> schema r1 @ schema r2
    | AEmpty -> []
    | AScalar e -> of_preds [ (e.s_pred, e.s_name) ]
    | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema rs
    | ATuple ([], Concat) -> []
    | ATuple (r :: _, Concat) -> schema r
    | Relation r -> Relation.schema r
    | Range (p, p') ->
        let t =
          try Some (Prim_type.unify (to_type p) (to_type p')) with _ -> None
        in
        [ Name.create ?type_:t "range" ]
    | _ -> failwith "unsupported"
  in
  List.map schema ~f:(fun n -> { n with name = Simple (Name.name n) })

let schema_open schema r = schema_query_open schema r.node
let rec schema r = schema_open schema r

let annotate_schema r =
  let rec annot r =
    let node = query r.node in
    let s =
      schema_open
        (fun r -> r.meta#schema)
        {
          node;
          meta =
            object
              method schema = failwith "not set"
              method meta = r.meta
            end;
        }
    in
    {
      node;
      meta =
        object
          method meta = r.meta
          method schema = s
        end;
    }
  and query q = V.Map.query annot pred q
  and pred p = V.Map.pred annot pred p in
  annot r

let names r = schema r |> List.map ~f:Name.name

let to_type q =
  let rec to_type q = to_type_open schema to_type q in
  try to_type q
  with exn ->
    Error.create "Failed to compute type." (q, exn)
      [%sexp_of: _ annot pred * exn]
    |> Error.raise

let to_type_opt q = Or_error.try_with (fun () -> to_type q)
let to_select_list s = List.map s ~f:(fun n -> (`Name n, Name.name n))
let zero = List.map ~f:Name.zero
