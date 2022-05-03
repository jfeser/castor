open Core
open Ast
open Prim_type

type t = Name.t list [@@deriving compare, sexp]

type opt_t = (string option * Prim_type.t option) list
[@@deriving compare, sexp]

let pp = Fmt.Dump.list Name.pp
let scoped s = List.map ~f:(Name.scoped s)
let unscoped = List.map ~f:Name.unscoped

let to_type_open schema to_type = function
  | Sum p | Min p | Max p -> to_type p
  | Name n -> Name.type_exn n
  | Date _ | Unop ((Year | Month | Day), _) -> date_t
  | Int _ | Row_number
  | Unop ((Strlen | ExtractY | ExtractM | ExtractD), _)
  | Count
  | Binop (Strpos, _, _) ->
      int_t
  | Fixed _ | Avg _ -> fixed_t
  | Bool _ | Exists _
  | Binop ((Eq | Lt | Le | Gt | Ge | And | Or), _, _)
  | Unop (Not, _) ->
      bool_t
  | String _ | Substring _ -> string_t
  | Null None -> failwith "Untyped null."
  | Null (Some t) -> t
  | Binop ((Add | Sub | Mul | Div | Mod), p1, p2) | If (_, p1, p2) ->
      unify (to_type p1) (to_type p2)
  | First r -> (
      match schema r with
      | [ n ] -> Name.type_exn n
      | [] -> failwith "Unexpected empty schema."
      | _ -> failwith "Too many fields.")

let schema_open_opt schema r : (string option * _ option) list =
  let rec to_type p =
    to_type_open
      (fun r ->
        schema r |> List.map ~f:(fun (_, t) -> Name.create ?type_:t "dummy"))
      to_type p
  in
  let of_preds =
    List.map ~f:(fun (p, n) ->
        let t = Or_error.try_with (fun () -> to_type p) |> Or_error.ok in
        (Some n, t))
  in
  match r.node with
  | AList { l_values = r; _ }
  | DepJoin { d_rhs = r; _ }
  | Filter (_, r)
  | Dedup r
  | OrderBy { rel = r; _ } ->
      schema r
  | Select (x, _) | GroupBy (x, _, _) -> of_preds (Select_list.to_list x)
  | Join { r1; r2; _ } -> schema r1 @ schema r2
  | AOrderedIdx { oi_keys = r1; oi_values = r2; _ }
  | AHashIdx { hi_keys = r1; hi_values = r2; _ } ->
      let schema_r2 = schema r2 in
      let schema_r1 =
        List.filter (schema r1) ~f:(function
          | Some n, _ ->
              not
                (List.exists
                   ~f:(function
                     | Some n', _ -> String.(n = n') | None, _ -> false)
                   schema_r2)
          | None, _ -> true)
      in
      schema_r1 @ schema_r2
  | AEmpty -> []
  | AScalar e -> of_preds [ (e.s_pred, e.s_name) ]
  | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema rs
  | ATuple ([], Concat) -> []
  | ATuple (r :: _, Concat) -> schema r
  | Relation r ->
      Relation.schema_exn r
      |> List.map ~f:(fun (n, t) -> (Some (Name.name n), Some t))
  | Range (p, p') ->
      let t =
        try Some (Prim_type.unify (to_type p) (to_type p')) with _ -> None
      in
      [ (Some "range", t) ]

let rec schema_opt r = schema_open_opt schema_opt r

let schema_open schema r =
  let rec to_type p = to_type_open schema to_type p in
  let of_preds =
    List.map ~f:(fun (p, n) ->
        let t = Or_error.try_with (fun () -> to_type p) |> Or_error.ok in
        Name.create ?type_:t n)
  in
  match r.node with
  | AList { l_values = r; _ }
  | DepJoin { d_rhs = r; _ }
  | Filter (_, r)
  | Dedup r
  | OrderBy { rel = r; _ } ->
      schema r |> unscoped
  | Select (x, _) | GroupBy (x, _, _) ->
      Select_list.to_list x |> of_preds |> unscoped
  | Join { r1; r2; _ } -> schema r1 @ schema r2 |> unscoped
  | AOrderedIdx { oi_keys = r1; oi_values = r2; _ }
  | AHashIdx { hi_keys = r1; hi_values = r2; _ } ->
      let schema_r2 = schema r2 in
      let schema_r1 =
        List.filter (schema r1) ~f:(fun n ->
            not
              (List.mem ~equal:[%compare.equal: Name.t] schema_r2
              @@ Name.unscoped n))
      in
      schema_r1 @ schema_r2 |> unscoped
  | AEmpty -> []
  | AScalar e -> of_preds [ (e.s_pred, e.s_name) ] |> unscoped
  | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema rs |> unscoped
  | ATuple ([], Concat) -> []
  | ATuple (r :: _, Concat) -> schema r |> unscoped
  | Relation r -> Relation.schema r |> unscoped
  | Range (p, p') ->
      let t =
        try Some (Prim_type.unify (to_type p) (to_type p')) with _ -> None
      in
      [ Name.create ?type_:t "range" ]

let schema_open_full schema r =
  match r.node with
  | AOrderedIdx { oi_keys = r1; oi_values = r2; _ }
  | AHashIdx { hi_keys = r1; hi_values = r2; _ } ->
      schema r1 @ schema r2 |> unscoped
  | _ -> schema_open schema r

let rec schema r = schema_open schema r
let rec schema_full r = schema_open_full schema_full r
let names r = schema r |> List.map ~f:Name.name

let to_type q =
  let rec to_type q = to_type_open schema to_type q in
  try to_type q
  with exn ->
    Error.create "Failed to compute type." (q, exn)
      [%sexp_of: _ annot pred * exn]
    |> Error.raise

let to_type_opt q = Or_error.try_with (fun () -> to_type q)
let to_select_list s = List.map s ~f:(fun n -> (Name n, Name.name n))
