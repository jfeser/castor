open Core
include Ast
include Comparator.Make (Ast)
include Abslayout_pp
module V = Visitors
module O : Comparable.Infix with type t := Ast.t = Comparable.Make (Ast)
open Schema

let pp = Abslayout_pp.pp

let scopes r =
  let visitor =
    object
      inherit [_] V.reduce
      inherit [_] Util.set_monoid (module String)
      method! visit_scope () s = Set.singleton (module String) s
    end
  in
  visitor#visit_t () r

let alpha_scopes r =
  let map =
    scopes r |> Set.to_list
    |> List.map ~f:(fun s -> (s, Fresh.name Global.fresh "s%d"))
    |> Map.of_alist_exn (module String)
  in
  let visitor =
    object
      inherit [_] V.endo

      method! visit_Name () p n =
        let open Option.Let_syntax in
        let name =
          let%bind s = Name.rel n in
          let%map s' = Map.find map s in
          Name (Name.copy ~scope:(Some s') n)
        in
        Option.value name ~default:p

      method! visit_scope () s = Map.find_exn map s
    end
  in
  visitor#visit_t () r

let wrap x = { node = x; meta = object end }
let select a b = wrap (Select (Select_list.of_list_exn a, strip_meta b))

let ensure_no_overlap_2 r1 r2 ss =
  if
    Set.inter (scopes r1)
      (Set.union (scopes r2) (Set.of_list (module String) ss))
    |> Set.is_empty
  then (r1, r2)
  else (alpha_scopes r1, r2)

let ensure_no_overlap_k rs =
  let open Collections in
  let all_scopes = List.map ~f:scopes rs in
  if Set.any_overlap (module String) all_scopes then List.map rs ~f:alpha_scopes
  else rs

let range a b = wrap (Range (a, b))

let dep_join a b c =
  let a, c = ensure_no_overlap_2 a c [ b ] in
  wrap (DepJoin { d_lhs = strip_meta a; d_alias = b; d_rhs = strip_meta c })

let dep_join' d = dep_join d.d_lhs d.d_alias d.d_rhs

let join a b c =
  let b, c = ensure_no_overlap_2 b c [] in
  wrap (Join { pred = a; r1 = strip_meta b; r2 = strip_meta c })

let filter a b =
  match Pred.kind a with
  | `Scalar -> wrap (Filter (a, strip_meta b))
  | `Agg | `Window ->
      Error.create "Aggregates not allowed in filter." a [%sexp_of: Pred.t]
      |> Error.raise

let group_by a b c = wrap (GroupBy (a, b, strip_meta c))
let dedup a = wrap (Dedup (strip_meta a))
let order_by a b = wrap (OrderBy { key = a; rel = strip_meta b })
let relation r = wrap (Relation r)
let empty = wrap AEmpty
let scalar p n = wrap (AScalar { s_pred = p; s_name = n })
let scalar' s = wrap (AScalar s)
let scalar_name n = scalar (Name n) (Name.name n)

let list a b c =
  let a, c = ensure_no_overlap_2 a c [ b ] in
  wrap (AList { l_keys = strip_meta a; l_scope = b; l_values = strip_meta c })

let list' l = list l.l_keys l.l_scope l.l_values

let tuple a b =
  let a = List.map ~f:strip_meta a in
  let a = ensure_no_overlap_k a in
  wrap (ATuple (a, b))

let hash_idx ?key_layout a b c d =
  let a, c = ensure_no_overlap_2 a c [ b ] in
  wrap
    (AHashIdx
       {
         hi_keys = strip_meta a;
         hi_values = strip_meta c;
         hi_scope = b;
         hi_lookup = d;
         hi_key_layout = key_layout;
       })

let hash_idx' h =
  hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
    h.hi_lookup

let ordered_idx ?key_layout a b c d =
  let a, c = ensure_no_overlap_2 a c [ b ] in
  wrap
    (AOrderedIdx
       {
         oi_keys = strip_meta a;
         oi_values = strip_meta c;
         oi_scope = b;
         oi_lookup = d;
         oi_key_layout = key_layout;
       })

let ordered_idx' o =
  ordered_idx ?key_layout:o.oi_key_layout o.oi_keys o.oi_scope o.oi_values
    o.oi_lookup

let rec and_ = function
  | [] -> Bool true
  | [ x ] -> x
  | x :: xs -> Binop (And, x, and_ xs)

let name r =
  match r.node with
  | Select _ -> "select"
  | Filter _ -> "filter"
  | DepJoin _ -> "depjoin"
  | Join _ -> "join"
  | GroupBy _ -> "group_by"
  | Dedup _ -> "dedup"
  | OrderBy _ -> "order_by"
  | Relation _ -> "scan"
  | AEmpty -> "empty"
  | AScalar _ -> "scalar"
  | AList _ -> "list"
  | ATuple _ -> "tuple"
  | AHashIdx _ -> "hash_idx"
  | AOrderedIdx _ -> "ordered_idx"
  | Range _ -> "range"
  | Call _ -> "call"

type error = [ `Parse_error of string * int * int ] [@@deriving sexp]

let pp_err f fmt = function
  | `Parse_error (msg, line, col) ->
      Fmt.pf fmt "Parse error: %s (line: %d, col: %d)" msg line col
  | e -> f fmt e

let ok_exn x =
  Result.map_error ~f:(Fmt.str "%a" (pp_err Fmt.nop)) x |> Result.ok_or_failwith

let of_lexbuf lexbuf =
  try Ok (Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf) with
  | Parser_utils.ParseError (msg, line, col) ->
      Error (`Parse_error (msg, line, col))
  | _ ->
      Error
        (`Parse_error (Parser_utils.error "Unknown error" lexbuf.lex_curr_p))

let of_channel ch = of_lexbuf (Lexing.from_channel ch)
let of_channel_exn x = of_channel x |> ok_exn
let of_string s = of_lexbuf (Lexing.from_string s)
let of_string_exn x = of_string x |> ok_exn

let name_of_lexbuf lexbuf =
  try Ok (Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf)
  with Parser_utils.ParseError (msg, line, col) ->
    Error (`Parse_error (msg, line, col))

let name_of_string s = name_of_lexbuf (Lexing.from_string s)
let name_of_string_exn s = name_of_string s |> ok_exn
let names r = (new V.names_visitor)#visit_t () r

let subst ctx =
  let v =
    object
      inherit [_] V.endo

      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> x | None -> this
    end
  in
  v#visit_t ()

let select_kind l =
  if List.exists l ~f:(fun (p, _) -> Poly.(Pred.kind p = `Agg)) then `Agg
  else `Scalar

let order_open order r =
  match r.node with
  | OrderBy { key; _ } -> key
  | AOrderedIdx { oi_keys = r; _ } ->
      schema r |> List.map ~f:(fun n -> (Name n, Asc))
  | Filter (_, r) | AHashIdx { hi_values = r; _ } -> order r
  | ATuple (rs, Cross) -> List.map ~f:order rs |> List.concat
  | Select (ps, r) ->
      (* perform renaming through select *)
      List.filter_map (order r) ~f:(fun (p, d) ->
          List.find_map ps ~f:(fun (p', n) ->
              if [%equal: _ pred] p p' then Some (Name (Name.create n), d)
              else None))
  | AList { l_keys = r; l_values = r'; _ } ->
      let open Name.O in
      let s' = schema r' and eq' = r'.meta#eq in
      List.filter_map (order r) ~f:(function
        | Name n, dir ->
            if List.mem ~equal:( = ) s' n then Some (Name n, dir)
            else
              Set.find_map eq' ~f:(fun (n', n'') ->
                  if n = n' then Some (Name n'', dir)
                  else if n = n'' then Some (Name n', dir)
                  else None)
        | _ -> None)
  | DepJoin _ | Join _ | GroupBy _ | Dedup _ | Relation _ | AEmpty
  | ATuple (_, (Zip | Concat))
  | AScalar _ | Range _ ->
      []
  | _ -> failwith "unsupported"

let rec order_of r = order_open order_of r

let annotate_key_layouts r =
  let key_layout schema =
    match List.map schema ~f:(fun n -> scalar_name n) with
    | [] -> failwith "empty schema"
    | [ x ] -> x
    | xs -> tuple xs Cross
  in
  let annotator =
    object (self : 'a)
      inherit [_] V.map

      method! visit_AHashIdx () h =
        let h = self#visit_hash_idx () h in
        let hi_key_layout =
          Option.first_some h.hi_key_layout
            (Some (key_layout @@ scoped h.hi_scope @@ schema h.hi_keys))
        in
        AHashIdx { h with hi_key_layout }

      method! visit_AOrderedIdx () o =
        let o = self#visit_ordered_idx () o in
        let oi_key_layout =
          Option.first_some o.oi_key_layout
            (Some (key_layout @@ scoped o.oi_scope @@ schema o.oi_keys))
        in
        AOrderedIdx { o with oi_key_layout }
    end
  in
  annotator#visit_t () r

(** Collect all named relations in an expression. *)
(* let aliases = *)
(*   let visitor = *)
(*     object (self : 'a) *)
(*       inherit [_] V.reduce *)
(*       method zero = Map.empty (module Name) *)
(*       method one k v = Map.singleton (module Name) k v *)

(*       method plus = *)
(*         Map.merge ~f:(fun ~key:_ -> function *)
(*           | `Left r | `Right r -> Some r *)
(*           | `Both (r1, r2) -> *)
(*               if Pred.O.(r1 = r2) then Some r1 *)
(*               else failwith "Multiple relations with same alias") *)

(*       method! visit_Exists () _ = self#zero *)
(*       method! visit_First () _ = self#zero *)

(*       method! visit_Select () (ps, r) = *)
(*         match select_kind ps with *)
(*         | `Scalar -> *)
(*             List.fold_left ps ~init:(self#visit_t () r) ~f:(fun m p -> *)
(*                 match p with *)
(*                 | As_pred (p, n) -> self#plus (self#one (Name.create n) p) m *)
(*                 | _ -> m) *)
(*         | `Agg -> self#zero *)
(*     end *)
(*   in *)
(*   visitor#visit_t () *)

(* let relations = *)
(*   let visitor = *)
(*     object *)
(*       inherit [_] V.reduce *)
(*       inherit [_] Util.set_monoid (module Relation) *)
(*       method! visit_Relation () r = Set.singleton (module Relation) r *)
(*     end *)
(*   in *)
(*   visitor#visit_t () *)

let h_key_layout { hi_key_layout; hi_keys; _ } =
  match hi_key_layout with
  | Some l -> strip_meta l
  | None -> (
      match
        List.map (schema hi_keys) ~f:(fun n -> scalar (Name n) (Name.name n))
      with
      | [] -> failwith "empty schema"
      | [ x ] -> x
      | xs -> tuple xs Cross)

(* let o_key_layout (oi_keys, _, { oi_key_layout; _ }) = *)
(*   match oi_key_layout with *)
(*   | Some l -> strip_meta l *)
(*   | None -> ( *)
(*       match List.map (schema oi_keys) ~f:(fun n -> scalar (Name n)) with *)
(*       | [] -> failwith "empty schema" *)
(*       | [ x ] -> x *)
(*       | xs -> tuple xs Cross) *)

let rec hoist_meta { node; meta } =
  { node = hoist_meta_query node; meta = meta#meta }

and hoist_meta_query q = V.Map.query hoist_meta hoist_meta_pred q
and hoist_meta_pred p = V.Map.pred hoist_meta hoist_meta_pred p
