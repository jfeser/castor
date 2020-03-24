open Ast
open Prim_type

type t = Name.t list [@@deriving compare, sexp]

let scoped s = List.map ~f:(Name.scoped s)

let unscoped = List.map ~f:Name.unscoped

let to_name = function
  | Name n ->
      (* NOTE: Scopes are not emitted as part of schemas. *)
      Some (Name.copy ~scope:None n)
  | As_pred (_, n) -> Some (Name.create n)
  | _ -> None

let to_type_open schema to_type = function
  | As_pred (p, _) | Sum p | Min p | Max p -> to_type p
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
      | _ -> failwith "Too many fields." )

let schema_open schema r =
  let rec to_type p = to_type_open schema to_type p in
  let of_preds =
    List.map ~f:(fun p ->
        let t = Or_error.try_with (fun () -> to_type p) |> Or_error.ok in
        match to_name p with
        | Some n -> Name.copy ~type_:t n
        | None ->
            Log.err (fun m ->
                m "Tried to get schema of unnamed predicate %a."
                  Abslayout_pp.pp_pred p);
            Name.create ?type_:t (Fresh.name Global.fresh "x%d"))
  in
  match r.node with
  | AList (_, r)
  | DepJoin { d_rhs = r; _ }
  | Filter (_, r)
  | Dedup r
  | OrderBy { rel = r; _ } ->
      schema r |> unscoped
  | Select (x, _) | GroupBy (x, _, _) -> of_preds x |> unscoped
  | Join { r1; r2; _ } -> schema r1 @ schema r2 |> unscoped
  | AOrderedIdx (r1, r2, _) | AHashIdx { hi_keys = r1; hi_values = r2; _ } ->
      let schema_r2 = schema r2 in
      let schema_r1 =
        List.filter (schema r1) ~f:(fun n ->
            not
              (List.mem ~equal:[%compare.equal: Name.t] schema_r2
                 (Name.unscoped n)))
      in
      schema_r1 @ schema_r2 |> unscoped
  | AEmpty -> []
  | AScalar e -> of_preds [ e ] |> unscoped
  | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema rs |> unscoped
  | ATuple ([], Concat) -> []
  | ATuple (r :: _, Concat) -> schema r |> unscoped
  | As (n, r) -> scoped n (schema r)
  | Relation r -> Relation.schema r |> unscoped
  | Range (p, p') ->
      let t = Prim_type.unify (to_type p) (to_type p') in
      [ Name.create ~type_:t "range" ]

let schema_open_full schema r =
  match r.node with
  | AOrderedIdx (r1, r2, _) | AHashIdx { hi_keys = r1; hi_values = r2; _ } ->
      schema r1 @ schema r2 |> unscoped
  | _ -> schema_open schema r

let rec schema r = schema_open schema r

let rec schema_full r = schema_open_full schema_full r

let names r = schema r |> List.map ~f:Name.name

let types r = schema r |> List.map ~f:Name.type_exn

let types_full r = schema_full r |> List.map ~f:Name.type_exn

let names_and_types r =
  schema r |> List.map ~f:(fun n -> (Name.name n, Name.type_exn n))

let rec to_type q =
  let rec to_type q = to_type_open schema to_type q in
  try to_type q
  with exn ->
    Error.create "Failed to compute type." (q, exn)
      [%sexp_of: _ annot pred * exn]
    |> Error.raise

let to_type_opt q = Or_error.try_with (fun () -> to_type q)

let to_select_list s = List.map s ~f:(fun n -> Name n)
