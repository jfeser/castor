include Ast
include Comparator.Make (Ast)
include Abslayout_pp
include Visitors

module O : Comparable.Infix with type t := Ast.t = Comparable.Make (Ast)

open Schema

let scope r = match r.node with As (n, _) -> Some n | _ -> None

let scope_exn r =
  Option.value_exn
    ~error:
      ( Error.of_lazy_t
      @@ lazy (Error.createf "Expected a scope on %a." pp_small_str r) )
    (scope r)

let strip_scope r = match r.node with As (_, r) -> r | _ -> r

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

let wrap x = { node = x; meta = object end }

let select a b =
  (* Check that the names in the select list are unique. *)
  List.filter_map a ~f:Pred.to_name
  |> List.find_a_dup ~compare:[%compare: Name.t]
  |> Option.iter ~f:(fun dup ->
         Error.create "Select list contains duplicate names" dup
           [%sexp_of: Name.t]
         |> Error.raise);

  wrap (Select (a, strip_meta b))

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

let names r = (new names_visitor)#visit_t () r

let subst ctx =
  let v =
    object
      inherit [_] endo

      method! visit_Name _ this v =
        match Map.find ctx v with Some x -> x | None -> this
    end
  in
  v#visit_t ()

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

let select_kind l =
  if List.exists l ~f:(fun p -> Poly.(Pred.kind p = `Agg)) then `Agg
  else `Scalar

let order_open order r =
  match r.node with
  | OrderBy { key; _ } -> key
  | AOrderedIdx (r, _, _) -> schema r |> List.map ~f:(fun n -> (Name n, Asc))
  | Filter (_, r) | AHashIdx { hi_values = r; _ } -> order r
  | ATuple (rs, Cross) -> List.map ~f:order rs |> List.concat
  | Select (ps, r) ->
      List.filter_map (order r) ~f:(fun (p, d) ->
          List.find_map ps ~f:(function
            | As_pred (p', n) when [%compare.equal: _ pred] p p' ->
                Some (Name (Name.create n), d)
            | p' when [%compare.equal: _ pred] p p' -> Some (p', d)
            | _ -> None))
  | AList (r, r') ->
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
  | AScalar _ | As _ | Range _ ->
      []

let rec order_of r = order_open order_of r

let order_of r =
  let r =
    Equiv.annotate r
    |> map_meta (fun eq ->
           object
             method eq = eq
           end)
  in
  (order_of r :> (< > annot pred * order) list)

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
            (Some (key_layout @@ scoped h.hi_scope @@ schema h.hi_keys))
        in
        AHashIdx { h with hi_key_layout }

      method! visit_AOrderedIdx () (x, y, o) =
        let x = self#visit_t () x in
        let y = self#visit_t () y in
        let oi_key_layout =
          Option.first_some o.oi_key_layout (Some (key_layout @@ schema x))
        in
        AOrderedIdx (x, y, { o with oi_key_layout })
    end
  in
  annotator#visit_t () r

let strip_unused_as q =
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
  visitor#visit_t () q

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

let h_key_layout { hi_key_layout; hi_keys; _ } =
  match hi_key_layout with
  | Some l -> strip_meta l
  | None -> (
      match List.map (schema hi_keys) ~f:(fun n -> scalar (Name n)) with
      | [] -> failwith "empty schema"
      | [ x ] -> x
      | xs -> tuple xs Cross )

let o_key_layout (oi_keys, _, { oi_key_layout; _ }) =
  match oi_key_layout with
  | Some l -> strip_meta l
  | None -> (
      match List.map (schema oi_keys) ~f:(fun n -> scalar (Name n)) with
      | [] -> failwith "empty schema"
      | [ x ] -> x
      | xs -> tuple xs Cross )
