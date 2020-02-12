open! Core
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

let rec to_type_open schema p =
  let to_type = to_type_open schema in
  match p with
  | As_pred (p, _) | Sum p | Min p | Max p -> to_type p
  | Name n -> Name.Meta.(find_exn n type_)
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
      | [ n ] -> Name.Meta.(find_exn n type_)
      | [] -> failwith "Unexpected empty schema."
      | _ -> failwith "Too many fields." )

let to_type_opt_open schema p =
  Or_error.try_with (fun () -> to_type_open schema p)

let schema_open schema r =
  let to_type_opt = to_type_opt_open schema in
  let to_type = to_type_open schema in
  let of_preds =
    List.map ~f:(fun p ->
        let t = to_type_opt p |> Or_error.ok in
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
      schema r1 @ schema r2 |> unscoped
  | AEmpty -> []
  | AScalar e -> of_preds [ e ] |> unscoped
  | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema rs |> unscoped
  | ATuple ([], Concat) -> []
  | ATuple (r :: _, Concat) -> schema r |> unscoped
  | As (n, r) -> scoped n (schema r)
  | Relation { r_schema = Some schema; _ } -> schema |> unscoped
  | Relation { r_name; r_schema = None; _ } ->
      Error.(
        create "Missing schema annotation." r_name [%sexp_of: string] |> raise)
  | Range (p, p') ->
      let t = Prim_type.unify (to_type p) (to_type p') in
      [ Name.create ~type_:t "range" ]

let rec schema r = schema_open schema r

let schema_exn r =
  let s = schema r in
  List.iter ~f:(fun n -> Name.Meta.(find_exn n type_) |> ignore) s;
  s

let to_type q = to_type_open schema q

let to_type_opt q = to_type_opt_open schema q

let to_select_list s = List.map s ~f:(fun n -> Name n)
