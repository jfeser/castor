open! Core
open Abslayout0

type t = Name.t list [@@deriving sexp]

let scoped s = List.map ~f:(Name.scoped s)

let unscoped = List.map ~f:Name.unscoped

let to_name = function
  | Name n ->
      (* NOTE: Scopes are not emitted as part of schemas. *)
      Some (Name.copy ~scope:None n)
  | As_pred (_, n) -> Some (Name.create n)
  | _ -> None

let rec to_type =
  let open Type.PrimType in
  function
  | As_pred (p, _) -> to_type p
  | Name n -> Name.Meta.(find_exn n type_)
  | Date _ | Unop ((Year | Month | Day), _) -> date_t
  | Int _ | Row_number | Unop ((Strlen | ExtractY | ExtractM | ExtractD), _) | Count
    ->
      int_t
  | Fixed _ | Avg _ -> fixed_t
  | Bool _ | Exists _
   |Binop ((Eq | Lt | Le | Gt | Ge | And | Or), _, _)
   |Unop (Not, _) ->
      bool_t
  | String _ -> string_t
  | Null None -> failwith "Untyped null."
  | Null (Some t) -> t
  | Binop ((Add | Sub | Mul | Div | Mod), p1, p2) ->
      let s1 = to_type p1 in
      let s2 = to_type p2 in
      Type.PrimType.unify s1 s2
  | Binop (Strpos, _, _) -> int_t
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
  | Substring _ -> string_t

and schema_exn r =
  let of_preds =
    List.map ~f:(fun p ->
        let t = to_type p in
        match to_name p with
        | Some n -> Name.copy ~type_:(Some t) n
        | None ->
            Log.err (fun m ->
                m "Tried to get schema of unnamed predicate %a." pp_pred p) ;
            Name.create ~type_:t (Fresh.name Global.fresh "x%d"))
  in
  match r.node with
  | AList (_, r) | DepJoin {d_rhs= r; _} -> schema_exn r |> unscoped
  | Select (x, _) | GroupBy (x, _, _) -> of_preds x |> unscoped
  | Filter (_, r) | Dedup r | OrderBy {rel= r; _} -> schema_exn r |> unscoped
  | Join {r1; r2; _} -> schema_exn r1 @ schema_exn r2 |> unscoped
  | AOrderedIdx (r1, r2, _) | AHashIdx {hi_keys= r1; hi_values= r2; _} ->
      schema_exn r1 @ schema_exn r2 |> unscoped
  | AEmpty -> []
  | AScalar e -> of_preds [e] |> unscoped
  | ATuple (rs, (Cross | Zip)) -> List.concat_map ~f:schema_exn rs |> unscoped
  | ATuple ([], Concat) -> []
  | ATuple (r :: _, Concat) -> schema_exn r |> unscoped
  | As (n, r) -> scoped n (schema_exn r)
  | Relation {r_schema= Some schema; _} -> schema |> unscoped
  | Relation {r_name; r_schema= None; _} ->
      Error.(create "Missing schema annotation." r_name [%sexp_of: string] |> raise)
  | Range (p, p') ->
      let t = Type.PrimType.unify (to_type p) (to_type p') in
      [Name.create ~type_:t "range"]

let to_select_list s = List.map s ~f:(fun n -> Name n)
