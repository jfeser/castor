open! Core
open Collections
module M = Meta
include Ast
include Comparator.Make (Ast)
include Abslayout_pp
include Abslayout_visitors

module O : Comparable.Infix with type t := t = Comparable.Make (Ast)

let scope r = match r.node with As (n, _) -> Some n | _ -> None

let scope_exn r =
  Option.value_exn
    ~error:(Error.createf "Expected a scope on %a." pp_small_str r)
    (scope r)

let strip_scope r = match r.node with As (_, r) -> r | _ -> r

let strip_meta =
  let visitor =
    object
      inherit [_] map as super

      method! visit_t () t = { (super#visit_t () t) with meta = M.empty () }
    end
  in
  visitor#visit_t ()

let scopes r =
  let visitor =
    object
      inherit [_] reduce

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
      inherit [_] endo

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

let wrap x = { node = x; meta = M.empty () }

let select a b = wrap (Select (a, strip_meta b))

let ensure_no_overlap_2 r1 r2 ss =
  if
    Set.inter (scopes r1)
      (Set.union (scopes r2) (Set.of_list (module String) ss))
    |> Set.is_empty
  then (r1, r2)
  else (alpha_scopes r1, r2)

let ensure_no_overlap_k rs =
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

let scalar a = wrap (AScalar a)

let as_ a b = wrap (As (a, strip_meta b))

let list a b c =
  let a = strip_scope a in
  let a, c = ensure_no_overlap_2 a c [ b ] in
  wrap (AList (strip_meta (as_ b a), strip_meta c))

let list' (a, b) = list a (scope_exn a) b

let tuple a b =
  let a = List.map ~f:strip_meta a in
  let a = ensure_no_overlap_k a in
  wrap (ATuple (a, b))

let hash_idx ?key_layout a b c d =
  let a = strip_scope a in
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

let ordered_idx a b c d =
  let a = strip_scope a in
  let a, c = ensure_no_overlap_2 a c [ b ] in
  wrap (AOrderedIdx (strip_meta (as_ b a), strip_meta c, d))

let rec and_ = function
  | [] -> Bool true
  | [ x ] -> x
  | x :: xs -> Binop (And, x, and_ xs)

let schema_exn = Schema.schema_exn

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
  | As _ -> "as"
  | Range _ -> "range"

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Log.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
    raise e

let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let name_of_lexbuf_exn lexbuf =
  try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Log.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
    raise e

let name_of_string_exn s = name_of_lexbuf_exn (Lexing.from_string s)

let names r = names_visitor#visit_t () r

let subst ctx =
  let v =
    object
      inherit [_] endo

      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> x | None -> this
    end
  in
  v#visit_t ()

let rec pred_free p =
  let singleton = Set.singleton (module Name) in
  let visitor =
    object (self : 'a)
      inherit [_] reduce as super

      inherit [_] Util.set_monoid (module Name)

      method visit_subquery r = free r

      method! visit_Name () n = singleton n

      method! visit_pred () p =
        match p with
        | Exists r | First r -> self#visit_subquery r
        | _ -> super#visit_pred () p
    end
  in
  visitor#visit_pred () p

and free r =
  let empty = Set.empty (module Name) in
  let of_list = Set.of_list (module Name) in
  let union_list = Set.union_list (module Name) in
  let exposed r = of_list (schema_exn r) in
  let scope r s = Set.map (module Name) s ~f:(Name.copy ~scope:(Some r)) in
  let free_set =
    match r.node with
    | Relation _ | AEmpty | Range _ -> empty
    | AScalar p -> pred_free p
    | Select (ps, r') ->
        Set.O.(
          free r' || ((List.map ps ~f:pred_free |> union_list) - exposed r'))
    | Filter (p, r') ->
        Set.union (free r') (Set.diff (pred_free p) (exposed r'))
    | Dedup r' -> free r'
    | DepJoin { d_lhs; d_alias; d_rhs } ->
        Set.O.(free d_lhs || (free d_rhs - (exposed d_lhs |> scope d_alias)))
    | Join { pred; r1; r2 } ->
        Set.O.(
          free r1 || free r2 || (pred_free pred - (exposed r1 || exposed r2)))
    | GroupBy (ps, key, r') ->
        Set.O.(
          free r'
          || (List.map ps ~f:pred_free |> union_list || of_list key)
             - exposed r')
    | OrderBy { key; rel } ->
        Set.O.(
          free rel
          || (List.map key ~f:(fun (p, _) -> pred_free p) |> union_list)
             - exposed rel)
    | AList (r', r'') -> Set.O.(free r' || (free r'' - exposed r'))
    | AHashIdx h ->
        union_list
          [
            free h.hi_keys;
            Set.diff (free h.hi_values) (exposed h.hi_keys);
            List.map ~f:pred_free h.hi_lookup |> union_list;
          ]
    | AOrderedIdx (r', r'', m) ->
        let one_bound_free = function
          | Some (p, _) -> pred_free p
          | None -> empty
        in
        let bound_free (b1, b2) =
          Set.union (one_bound_free b1) (one_bound_free b2)
        in
        union_list
          ( [ free r'; Set.diff (free r'') (exposed r') ]
          @ List.map ~f:bound_free m.oi_lookup )
    | ATuple (rs, (Zip | Concat)) -> List.map rs ~f:free |> union_list
    | ATuple (rs, Cross) ->
        let n, _ =
          List.fold_left rs ~init:(empty, empty) ~f:(fun (n, e) r' ->
              let e' = exposed r' in
              let n' = Set.union n (Set.diff (free r') e) in
              (n', e'))
        in
        n
    | As (_, r') -> free r'
  in
  M.(set_m r free free_set);
  free_set

(** Annotate all subexpressions with its set of free names. *)
let annotate_free r =
  let visitor =
    object
      inherit [_] iter

      method! visit_t () r =
        let fs = free r in
        M.(set_m r free fs)
    end
  in
  visitor#visit_t () r

let exists_bare_relations r =
  let visitor =
    object (self : 'a)
      inherit [_] reduce as super

      inherit [_] Util.disj_monoid

      method! visit_t () r =
        match r.node with
        | As (_, { node = Relation _; _ }) -> self#zero
        | As (_, r') -> self#visit_t () r'
        | Relation _ -> true
        | _ -> super#visit_t () r
    end
  in
  visitor#visit_t () r

let validate r =
  if exists_bare_relations r then
    Error.of_string "Program contains bare relation references." |> Error.raise

let dedup_pairs = List.dedup_and_sort ~compare:[%compare: Name.t * Name.t]

let annotate_eq r =
  let visitor =
    object
      inherit [_] iter as super

      method! visit_As m n r =
        super#visit_As None n r;
        let m = Option.value_exn m in
        let schema = schema_exn r in
        let eqs =
          List.map schema ~f:(fun n' -> (n', Name.(create ~scope:n (name n'))))
          |> dedup_pairs
        in
        M.Direct.set_m m M.eq eqs

      method! visit_Filter m (p, r) =
        super#visit_Filter None (p, r);
        let m = Option.value_exn m in
        let r_eqs = M.(find_exn r eq) in
        let eqs = Pred.eqs p @ r_eqs |> dedup_pairs in
        M.Direct.set_m m M.eq eqs

      method! visit_Select m (ps, r) =
        super#visit_Select None (ps, r);
        let m = Option.value_exn m in
        let eqs =
          M.(find_exn r eq)
          |> List.filter_map ~f:(fun ((n, n') as eq) ->
                 List.find_map ps
                   ~f:
                     (let open Name.O in
                     function
                     | Name n'' when n'' = n' || n'' = n -> Some eq
                     | As_pred (Name n'', s) when n'' = n ->
                         Some (Name.create s, n')
                     | As_pred (Name n'', s) when n'' = n' ->
                         Some (n, Name.create s)
                     | _ -> None))
        in
        M.Direct.set_m m M.eq eqs

      method! visit_Join m ({ pred = p; r1; r2 } as j) =
        super#visit_Join None j;
        let m = Option.value_exn m in
        let r1_eqs = M.(find_exn r1 eq) in
        let r2_eqs = M.(find_exn r2 eq) in
        let eqs = Pred.eqs p @ r1_eqs @ r2_eqs |> dedup_pairs in
        M.Direct.set_m m M.eq eqs

      method! visit_AList m (r1, r2) =
        super#visit_AList None (r1, r2);
        let m = Option.value_exn m in
        let r1_eqs = M.(find_exn r1 eq) in
        let r2_eqs = M.(find_exn r2 eq) in
        let eqs = r1_eqs @ r2_eqs |> dedup_pairs in
        M.Direct.set_m m M.eq eqs

      method! visit_t _ ({ meta; _ } as r) =
        M.(set_m r eq []);
        super#visit_t (Some meta) r
    end
  in
  visitor#visit_t None r

let select_kind l =
  if List.exists l ~f:(fun p -> Poly.(Pred.kind p = `Agg)) then `Agg
  else `Scalar

class ['a] stage_iter =
  object (self : 'a)
    inherit [_] iter

    method! visit_AList phase (rk, rv) =
      self#visit_t `Compile rk;
      self#visit_t phase rv

    method! visit_AHashIdx phase h =
      List.iter h.hi_lookup ~f:(self#visit_pred phase);
      self#visit_t `Compile h.hi_keys;
      self#visit_t phase h.hi_values

    method! visit_AOrderedIdx phase (rk, rv, m) =
      let bound_iter = Option.iter ~f:(fun (p, _) -> self#visit_pred phase p) in
      List.iter m.oi_lookup ~f:(fun (b1, b2) ->
          bound_iter b1;
          bound_iter b2);
      self#visit_t `Compile rk;
      self#visit_t phase rv

    method! visit_AScalar _ p = self#visit_pred `Compile p
  end

exception Un_serial of string

let ops_serializable_exn r =
  let visitor =
    object
      inherit [_] stage_iter as super

      method! visit_t s r =
        super#visit_t s r;
        match (s, r.node) with
        | `Run, (Relation _ | GroupBy (_, _, _) | Join _ | OrderBy _ | Dedup _)
          ->
            raise
            @@ Un_serial
                 (Format.asprintf
                    "Cannot serialize: Bad operator in run-time position %a" pp
                    r)
        | _ -> ()
    end
  in
  visitor#visit_t `Run r

let names_serializable_exn r =
  let visitor =
    object
      inherit [_] stage_iter

      method! visit_Name s n =
        match Name.Meta.(find n stage) with
        | Some s' ->
            if Poly.(s <> s') then
              let stage =
                match s with `Compile -> "compile" | `Run -> "run"
              in
              raise
                (Un_serial
                   (Format.asprintf
                      "Cannot serialize: Found %a in %s time position."
                      Name.pp_with_stage n stage))
        | None -> Logs.warn (fun m -> m "Missing stage on %a" Name.pp n)
    end
  in
  visitor#visit_t `Run r

(** Return true if `r` is serializable. This function performs two checks:
    - `r` must not contain any compile time only operations in run time position.
    - Run-time names may only appear in run-time position and vice versa. *)
let is_serializeable r =
  try
    ops_serializable_exn r;
    names_serializable_exn r;
    Ok ()
  with Un_serial msg -> Error msg

let annotate_orders r =
  let rec annotate_orders r =
    let order =
      match r.node with
      | Select (ps, r) ->
          annotate_orders r
          |> List.filter_map ~f:(fun (p, d) ->
                 List.find_map ps ~f:(function
                   | As_pred (p', n) when [%compare.equal: pred] p p' ->
                       Some (Name (Name.create n), d)
                   | p' when [%compare.equal: pred] p p' -> Some (p', d)
                   | _ -> None))
      | Filter (_, r) | AHashIdx { hi_values = r; _ } -> annotate_orders r
      | DepJoin { d_lhs = r1; d_rhs = r2; _ } | Join { r1; r2; _ } ->
          annotate_orders r1 |> ignore;
          annotate_orders r2 |> ignore;
          []
      | GroupBy (_, _, r) | Dedup r ->
          annotate_orders r |> ignore;
          []
      | Relation _ | AEmpty -> []
      | OrderBy { key; rel } ->
          annotate_orders rel |> ignore;
          key
      | AScalar _ -> []
      | AList (r, r') ->
          let s' = schema_exn r' in
          let eq' = M.(find_exn r' eq) in
          annotate_orders r' |> ignore;
          let open Name.O in
          annotate_orders r
          |> List.filter_map ~f:(function
               | Name n, dir ->
                   if List.mem ~equal:( = ) s' n then Some (Name n, dir)
                   else
                     List.find_map eq' ~f:(fun (n', n'') ->
                         if n = n' then Some (Name n'', dir)
                         else if n = n'' then Some (Name n', dir)
                         else None)
               | _ -> None)
      | ATuple (rs, Cross) -> List.map ~f:annotate_orders rs |> List.concat
      | ATuple (rs, (Zip | Concat)) ->
          List.iter ~f:(fun r -> annotate_orders r |> ignore) rs;
          []
      | AOrderedIdx (r, _, _) ->
          schema_exn r |> List.map ~f:(fun n -> (Name n, Asc))
      | As _ | Range _ -> []
    in
    M.set_m r M.order order;
    order
  in
  annotate_eq r;
  annotate_orders r |> ignore

let order_of r =
  annotate_orders r;
  M.(find_exn r order)

let annotate_key_layouts r =
  let key_layout schema =
    match List.map schema ~f:(fun n -> scalar (Name n)) with
    | [] -> failwith "empty schema"
    | [ x ] -> x
    | xs -> tuple xs Cross
  in
  let annotator =
    object (self : 'a)
      inherit [_] map

      method! visit_AHashIdx () h =
        let h = self#visit_hash_idx () h in
        let hi_key_layout =
          Option.first_some h.hi_key_layout
            (Some (key_layout (schema_exn h.hi_keys)))
        in
        AHashIdx { h with hi_key_layout }

      method! visit_AOrderedIdx () ((x, y, ({ oi_key_layout; _ } as m)) as r) =
        let x = self#visit_t () x in
        let y = self#visit_t () y in
        match oi_key_layout with
        | Some _ -> AOrderedIdx r
        | None ->
            AOrderedIdx
              (x, y, { m with oi_key_layout = Some (key_layout (schema_exn x)) })
    end
  in
  annotator#visit_t () r

let strip_unused_as =
  let visitor =
    object (self)
      inherit [_] endo

      method skip_as r =
        match r.node with
        | As (n, r) -> as_ n (self#visit_t () r)
        | _ -> self#visit_t () r

      method! visit_AList () _ (rk, rv) =
        let rk = self#skip_as rk in
        let rv = self#visit_t () rv in
        AList (rk, rv)

      method! visit_AOrderedIdx () _ (rk, rv, m) =
        let rk = self#skip_as rk in
        let rv = self#visit_t () rv in
        let m = self#visit_ordered_idx () m in
        AOrderedIdx (rk, rv, m)

      method! visit_AScalar () _ =
        function
        | As_pred (p, n) -> AScalar (As_pred (self#visit_pred () p, n))
        | p -> AScalar (self#visit_pred () p)

      method! visit_As () _ _ r =
        Log.warn (fun m -> m "Removing misplaced as: %a" pp_small r);
        r.node
    end
  in
  visitor#visit_t ()

let list_to_depjoin rk rv =
  let scope = scope_exn rk in
  dep_join (strip_scope rk) scope rv

let hash_idx_to_depjoin h =
  let rk_schema = schema_exn h.hi_keys |> Schema.scoped h.hi_scope in
  let rv_schema = schema_exn h.hi_values in
  let key_pred =
    List.map2_exn rk_schema h.hi_lookup ~f:(fun p1 p2 ->
        Binop (Eq, Name p1, p2))
    |> Pred.conjoin
  in
  let slist = rk_schema @ rv_schema |> List.map ~f:(fun n -> Name n) in
  dep_join h.hi_keys h.hi_scope (select slist (filter key_pred h.hi_values))

let ordered_idx_to_depjoin rk rv m =
  let scope = scope_exn rk in
  let rk_schema = schema_exn rk in
  let rv_schema = schema_exn rv in
  let key_pred =
    let rk_schema = schema_exn rk in
    List.zip_exn rk_schema m.oi_lookup
    |> List.concat_map ~f:(fun (n, (lb, ub)) ->
           let p1 =
             Option.map lb ~f:(fun (p, b) ->
                 match b with
                 | `Closed -> [ Binop (Ge, Name n, p) ]
                 | `Open -> [ Binop (Gt, Name n, p) ])
             |> Option.value ~default:[]
           in
           let p2 =
             Option.map ub ~f:(fun (p, b) ->
                 match b with
                 | `Closed -> [ Binop (Le, Name n, p) ]
                 | `Open -> [ Binop (Lt, Name n, p) ])
             |> Option.value ~default:[]
           in
           p1 @ p2)
    |> Pred.conjoin
  in
  let slist = rk_schema @ rv_schema |> List.map ~f:(fun n -> Name n) in
  dep_join (strip_scope rk) scope (select slist (filter key_pred rv))

let ensure_alias r =
  let visitor =
    object (self)
      inherit [_] endo

      method! visit_Select () _ (ps, r) =
        Select
          ( List.map ps ~f:(fun p ->
                p |> self#visit_pred () |> Pred.ensure_alias),
            self#visit_t () r )

      method! visit_GroupBy () _ (ps, k, r) =
        GroupBy
          ( List.map ps ~f:(fun p ->
                p |> self#visit_pred () |> Pred.ensure_alias),
            k,
            self#visit_t () r )

      method! visit_AScalar () _ p =
        AScalar (self#visit_pred () p |> Pred.ensure_alias)
    end
  in
  visitor#visit_t () r

(** Collect all named relations in an expression. *)
let aliases =
  let visitor =
    object (self : 'a)
      inherit [_] reduce

      method zero = Map.empty (module Name)

      method one k v = Map.singleton (module Name) k v

      method plus =
        Map.merge ~f:(fun ~key:_ ->
          function
          | `Left r | `Right r -> Some r
          | `Both (r1, r2) ->
              if Pred.O.(r1 = r2) then Some r1
              else failwith "Multiple relations with same alias")

      method! visit_Exists () _ = self#zero

      method! visit_First () _ = self#zero

      method! visit_Select () (ps, r) =
        match select_kind ps with
        | `Scalar ->
            List.fold_left ps ~init:(self#visit_t () r) ~f:(fun m p ->
                match p with
                | As_pred (p, n) -> self#plus (self#one (Name.create n) p) m
                | _ -> m)
        | `Agg -> self#zero
    end
  in
  visitor#visit_t ()

let relations =
  let visitor =
    object
      inherit [_] reduce

      inherit [_] Util.set_monoid (module Relation)

      method! visit_Relation () r = Set.singleton (module Relation) r
    end
  in
  visitor#visit_t ()

(* let rename_relations r =
 *   let subst =
 *     relations r |> Set.to_list
 *     |> List.concat_map ~f:(fun r ->
 *            Relation.schema_exn r
 *            |> List.map ~f:(fun n ->
 *                   let n' = Name.name n ^ Fresh.name Global.fresh "_%d" in
 *                   (Name.name n, n')))
 *     |> Map.of_alist_exn (module String)
 *   in
 *   let 
 *   let visitor =
 *     object
 *       inherit [_] map
 * 
 *       method! visit_Name () n =
 *         match Map.find subst (Name.name n) with
 *         | Some n' -> Name (Name.copy n ~name:n')
 *         | None -> Name n
 *     end
 *   in
 *   visitor#visit_t () r *)

let h_key_layout { hi_key_layout; hi_keys; _ } =
  match hi_key_layout with
  | Some l -> l
  | None -> (
      match List.map (schema_exn hi_keys) ~f:(fun n -> scalar (Name n)) with
      | [] -> failwith "empty schema"
      | [ x ] -> x
      | xs -> tuple xs Cross )

let o_key_layout (oi_keys, _, { oi_key_layout; _ }) =
  match oi_key_layout with
  | Some l -> l
  | None -> (
      match List.map (schema_exn oi_keys) ~f:(fun n -> scalar (Name n)) with
      | [] -> failwith "empty schema"
      | [ x ] -> x
      | xs -> tuple xs Cross )
