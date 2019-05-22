open! Core
open Collections
include Abslayout0

let strip_meta =
  let visitor =
    object
      inherit [_] map as super

      method! visit_t () t = {(super#visit_t () t) with meta= Meta.empty ()}
    end
  in
  visitor#visit_t ()

let strip_scope r = match r.node with As (_, r) -> r | _ -> r

let scopes r =
  let visitor =
    object (self : 'a)
      inherit [_] reduce

      inherit [_] Util.list_monoid

      method! visit_As () n r = self#plus [n] (self#visit_t () r)
    end
  in
  visitor#visit_t () r

let alpha_scopes r =
  let map =
    scopes r
    |> List.dedup_and_sort ~compare:[%compare: string]
    |> List.map ~f:(fun s -> (s, Fresh.name Global.fresh "s%d"))
    |> Map.of_alist_exn (module String)
  in
  let visitor =
    object (self : 'a)
      inherit [_] endo

      method! visit_Name () p n =
        let open Option.Let_syntax in
        let name =
          let%bind s = Name.rel n in
          let%map s' = Map.find map s in
          Name (Name.copy ~scope:(Some s') n)
        in
        Option.value name ~default:p

      method! visit_As () _ s r = As (Map.find_exn map s, self#visit_t () r)
    end
  in
  visitor#visit_t () r

let wrap x = {node= x; meta= Meta.empty ()}

let select a b = wrap (Select (a, strip_meta b))

let has_scope_overlap rs ss =
  List.concat_map rs ~f:scopes @ ss |> List.contains_dup ~compare:[%compare: string]

let dep_join a b c =
  let a, c =
    if has_scope_overlap [a; c] [b] then (alpha_scopes a, alpha_scopes c) else (a, c)
  in
  wrap (DepJoin {d_lhs= strip_meta a; d_alias= b; d_rhs= strip_meta c})

let dep_join' x = dep_join x.d_lhs x.d_alias x.d_rhs

let join a b c = wrap (Join {pred= a; r1= strip_meta b; r2= strip_meta c})

let filter a b = wrap (Filter (a, strip_meta b))

let group_by a b c = wrap (GroupBy (a, b, strip_meta c))

let dedup a = wrap (Dedup (strip_meta a))

let order_by a b = wrap (OrderBy {key= a; rel= strip_meta b})

let relation r = wrap (Relation r)

let empty = wrap AEmpty

let scalar a = wrap (AScalar a)

let as_ a b = wrap (As (a, strip_meta b))

let list a b c =
  let a = strip_scope a in
  let a, c =
    if has_scope_overlap [a; c] [b] then (alpha_scopes a, alpha_scopes c) else (a, c)
  in
  wrap (AList (strip_meta (as_ b a), strip_meta c))

let tuple a b = wrap (ATuple (List.map ~f:strip_meta a, b))

let hash_idx ?key_layout a b c d =
  let a = strip_scope a in
  let a, c =
    if has_scope_overlap [a; c] [b] then (alpha_scopes a, alpha_scopes c) else (a, c)
  in
  wrap
    (AHashIdx
       { hi_keys= strip_meta a
       ; hi_values= strip_meta c
       ; hi_scope= b
       ; hi_lookup= d
       ; hi_key_layout= key_layout })

let h_key_layout {hi_key_layout; _} =
  Option.value_exn ~message:"No key layout annotation found." hi_key_layout

let hash_idx' h =
  hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values h.hi_lookup

let ordered_idx a b c d =
  let a = strip_scope a in
  let a, c =
    if has_scope_overlap [a; c] [b] then (alpha_scopes a, alpha_scopes c) else (a, c)
  in
  wrap (AOrderedIdx (strip_meta (as_ b a), strip_meta c, d))

let o_key_layout {oi_key_layout; _} =
  Option.value_exn ~message:"No key layout annotation found." oi_key_layout

let scope r = match r.node with As (n, _) -> Some n | _ -> None

let rec and_ = function
  | [] -> Bool true
  | [x] -> x
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

let scope_exn r =
  Option.value_exn
    ~error:(Error.createf "Expected a scope on %a." pp_small_str r)
    (scope r)

let of_lexbuf_exn lexbuf =
  try Ralgebra_parser.ralgebra_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let of_channel_exn ch = of_lexbuf_exn (Lexing.from_channel ch)

let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

let name_of_lexbuf_exn lexbuf =
  try Ralgebra_parser.name_eof Ralgebra_lexer.token lexbuf
  with Parser_utils.ParseError (msg, line, col) as e ->
    Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
    raise e

let name_of_string_exn s = name_of_lexbuf_exn (Lexing.from_string s)

let names_visitor =
  object (self : 'a)
    inherit [_] reduce as super

    method zero = Set.empty (module Name)

    method plus = Set.union

    method! visit_Name () n = Set.singleton (module Name) n

    method! visit_pred () p =
      match p with
      | Exists _ | First _ -> self#zero
      | Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null _ | Unop _
       |Binop _ | As_pred _ | Count | Sum _ | Avg _ | Min _ | Max _
       |If (_, _, _)
       |Substring (_, _, _) ->
          super#visit_pred () p
  end

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
        | Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null _ | Unop _
         |Binop _ | As_pred _ | Count | Sum _ | Avg _ | Min _ | Max _
         |If (_, _, _)
         |Substring (_, _, _) ->
            super#visit_pred () p
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
    | Relation _ | AEmpty -> empty
    | AScalar p -> pred_free p
    | Select (ps, r') ->
        Set.O.(free r' || ((List.map ps ~f:pred_free |> union_list) - exposed r'))
    | Filter (p, r') -> Set.union (free r') (Set.diff (pred_free p) (exposed r'))
    | Dedup r' -> free r'
    | DepJoin {d_lhs; d_alias; d_rhs} ->
        Set.O.(free d_lhs || (free d_rhs - (exposed d_lhs |> scope d_alias)))
    | Join {pred; r1; r2} ->
        Set.O.(free r1 || free r2 || (pred_free pred - (exposed r1 || exposed r2)))
    | GroupBy (ps, key, r') ->
        Set.O.(
          free r'
          || ((List.map ps ~f:pred_free |> union_list || of_list key) - exposed r'))
    | OrderBy {key; rel} ->
        Set.O.(
          free rel
          || (List.map key ~f:(fun (p, _) -> pred_free p) |> union_list)
             - exposed rel)
    | AList (r', r'') -> Set.O.(free r' || (free r'' - exposed r'))
    | AHashIdx h ->
        union_list
          [ free h.hi_keys
          ; Set.diff (free h.hi_values) (exposed h.hi_keys)
          ; List.map ~f:pred_free h.hi_lookup |> union_list ]
    | AOrderedIdx (r', r'', {lookup_low; lookup_high; _}) ->
        union_list
          [ free r'
          ; Set.diff (free r'') (exposed r')
          ; pred_free lookup_low
          ; pred_free lookup_high ]
    | ATuple (rs, (Zip | Concat)) -> List.map rs ~f:free |> union_list
    | ATuple (rs, Cross) ->
        let n, _ =
          List.fold_left rs ~init:(empty, empty) ~f:(fun (n, e) r' ->
              let e' = exposed r' in
              let n' = Set.union n (Set.diff (free r') e) in
              (n', e') )
        in
        n
    | As (_, r') -> free r'
  in
  Meta.(set_m r free free_set) ;
  free_set

(** Annotate all subexpressions with its set of free names. *)
let annotate_free r =
  let visitor =
    object
      inherit [_] iter

      method! visit_t () r =
        let fs = free r in
        Meta.(set_m r free fs)
    end
  in
  visitor#visit_t () r

let dedup_pairs = List.dedup_and_sort ~compare:[%compare: Name.t * Name.t]

let exists_bare_relations r =
  let visitor =
    object (self : 'a)
      inherit [_] reduce as super

      inherit [_] Util.disj_monoid

      method! visit_t () r =
        match r.node with
        | As (_, {node= Relation _; _}) -> self#zero
        | As (_, r') -> self#visit_t () r'
        | Relation _ -> true
        | _ -> super#visit_t () r
    end
  in
  visitor#visit_t () r

let validate r =
  if exists_bare_relations r then
    Error.of_string "Program contains bare relation references." |> Error.raise

(** Collect all named relations in an expression. *)
let aliases =
  let visitor =
    object (self : 'a)
      inherit [_] reduce

      method zero = Map.empty (module String)

      method one k v = Map.singleton (module String) k v

      method plus =
        Map.merge ~f:(fun ~key:_ -> function
          | `Left r | `Right r -> Some r
          | `Both (r1, r2) ->
              if O.(r1 = r2) then Some r1
              else failwith "Multiple relations with same alias" )

      method! visit_Exists () _ = self#zero

      method! visit_First () _ = self#zero

      method! visit_As () n r = self#plus (self#one n (as_ n r)) (self#visit_t () r)

      method! visit_Relation () r = self#one r.r_name (relation r)
    end
  in
  visitor#visit_t ()

module Pred = struct
  type a = t [@@deriving compare, hash, sexp_of]

  module T = struct
    type t = Abslayout0.pred =
      | Name of Name.t
      | Int of int
      | Fixed of Fixed_point.t
      | Date of Date.t
      | Bool of bool
      | String of string
      | Null of Type.PrimType.t option
      | Unop of (unop * pred)
      | Binop of (binop * pred * pred)
      | As_pred of (pred * string)
      | Count
      | Sum of pred
      | Avg of pred
      | Min of pred
      | Max of pred
      | If of pred * pred * pred
      | First of a
      | Exists of a
      | Substring of pred * pred * pred
    [@@deriving compare, hash, sexp_of, variants]
  end

  include T
  include Comparator.Make (T)

  let pp = pp_pred

  let names r = names_visitor#visit_pred () r

  let of_value = Value.to_pred

  let rec conjoin = function
    | [] -> Bool true
    | [p] -> p
    | p :: ps -> Binop (And, p, conjoin ps)

  let collect_aggs p =
    let visitor =
      object (self : 'a)
        inherit [_] Abslayout0.mapreduce

        inherit [_] Util.list_monoid

        method private visit_Agg kind p =
          let n = kind ^ Fresh.name Global.fresh "%d" in
          (Name (Name.create n), [(n, p)])

        method! visit_Sum () p = self#visit_Agg "sum" (Sum p)

        method! visit_Count () = self#visit_Agg "count" Count

        method! visit_Min () p = self#visit_Agg "min" (Min p)

        method! visit_Max () p = self#visit_Agg "max" (Max p)

        method! visit_Avg () p = self#visit_Agg "avg" (Avg p)
      end
    in
    visitor#visit_pred () p

  let rec conjuncts = function
    | Binop (And, p1, p2) -> conjuncts p1 @ conjuncts p2
    | p -> [p]

  let constants schema p =
    let module M = struct
      module T = struct
        type t = pred [@@deriving compare, sexp_of]

        let t_of_sexp _ = failwith "Unimplemented."
      end

      include T
      include Comparable.Make (T)
    end in
    let schema = Set.of_list (module Name) schema in
    let empty = Set.empty (module M) in
    let singleton = Set.singleton (module M) in
    let visitor =
      object
        inherit [_] reduce as super

        inherit [_] Util.set_monoid (module M)

        method! visit_Name () n =
          if Set.mem schema n then empty else singleton (Name n)

        method! visit_Int () x = singleton (Int x)

        method! visit_Fixed () x = singleton (Fixed x)

        method! visit_Date () x = singleton (Date x)

        method! visit_Bool () x = singleton (Bool x)

        method! visit_String () x = singleton (String x)

        method! visit_Null () x = singleton (Null x)

        method! visit_As_pred () args =
          let p, _ = args in
          let ps = super#visit_As_pred () args in
          if Set.mem ps p then Set.add ps (As_pred args) else ps

        method! visit_Substring () p1 p2 p3 =
          let ps = super#visit_Substring () p1 p2 p3 in
          if Set.mem ps p1 && Set.mem ps p2 && Set.mem ps p3 then
            Set.add ps (Substring (p1, p2, p3))
          else ps

        method! visit_First () r =
          let free = Meta.(find_exn r free) in
          if Set.is_empty (Set.inter free schema) then singleton (First r)
          else empty

        method! visit_Exists () r =
          let free = Meta.(find_exn r free) in
          if Set.is_empty (Set.inter free schema) then singleton (Exists r)
          else empty

        method! visit_If () p1 p2 p3 =
          let ps = super#visit_If () p1 p2 p3 in
          if Set.mem ps p1 && Set.mem ps p2 && Set.mem ps p3 then
            Set.add ps (If (p1, p2, p3))
          else ps

        method! visit_Binop () args =
          let ps = super#visit_Binop () args in
          let _, p1, p2 = args in
          if Set.mem ps p1 && Set.mem ps p2 then Set.add ps (Binop args) else ps

        method! visit_Unop () args =
          let ps = super#visit_Unop () args in
          let _, p = args in
          if Set.mem ps p then Set.add ps (Unop args) else ps
      end
    in
    visitor#visit_pred () p |> Set.to_list

  let eqs p =
    let visitor =
      object (self : 'a)
        inherit [_] reduce

        method zero = []

        method plus = ( @ )

        method! visit_Binop () (op, p1, p2) =
          match (op, p1, p2) with
          | Eq, Name n1, Name n2 -> [(n1, n2)]
          | And, p1, p2 -> self#plus (self#visit_pred () p1) (self#visit_pred () p2)
          | _ -> self#zero
      end
    in
    visitor#visit_pred () p |> dedup_pairs

  let remove_as p =
    let visitor =
      object
        inherit [_] map

        method! visit_As_pred () (p, _) = p
      end
    in
    visitor#visit_pred () p

  let kind p =
    let visitor =
      object
        inherit [_] reduce

        inherit [_] Util.disj_monoid

        method! visit_Sum () _ = true

        method! visit_Avg () _ = true

        method! visit_Min () _ = true

        method! visit_Max () _ = true

        method! visit_Count () = true
      end
    in
    if visitor#visit_pred () p then `Agg else `Scalar

  let of_lexbuf_exn lexbuf =
    try Ralgebra_parser.expr_eof Ralgebra_lexer.token lexbuf
    with Parser_utils.ParseError (msg, line, col) as e ->
      Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
      raise e

  let of_string_exn s = of_lexbuf_exn (Lexing.from_string s)

  let subst ctx =
    let v =
      object
        inherit [_] endo

        method! visit_Name _ this v =
          match Map.find ctx v with Some x -> x | None -> this
      end
    in
    v#visit_pred ()

  let scoped names scope p =
    let ctx =
      List.map names ~f:(fun n -> (n, Name (Name.scoped scope n)))
      |> Map.of_alist_exn (module Name)
    in
    subst ctx p

  let unscoped scope p =
    let v =
      object
        inherit [_] endo

        method! visit_Name _ this n =
          match Name.rel n with
          | Some s ->
              if String.(s = scope) then Name (Name.copy ~scope:None n) else this
          | None -> this
      end
    in
    v#visit_pred () p

  (** Return the set of relations which have fields in the tuple produced by
     this expression. *)
  let relations p =
    let rels = ref [] in
    let f =
      object
        inherit [_] iter

        method! visit_Name () n =
          match Name.rel n with Some r -> rels := r :: !rels | None -> ()

        method visit_name () _ = ()

        method visit_'m () _ = ()
      end
    in
    f#visit_pred () p ; !rels

  let to_type = Schema.to_type

  let to_name = Schema.to_name

  (** Ensure that a predicate is decorated with an alias. If the predicate is
     nameless, then the alias will be fresh. *)
  let ensure_alias = function
    | As_pred _ as p -> p
    | p -> (
      match to_name p with
      | Some n -> As_pred (p, Name.name n)
      | None -> As_pred (p, Fresh.name Global.fresh "a%d") )
end

let annotate_eq r =
  let visitor =
    object
      inherit [_] iter as super

      method! visit_As m n r =
        super#visit_As None n r ;
        let m = Option.value_exn m in
        let schema = schema_exn r in
        let eqs =
          List.map schema ~f:(fun n' -> (n', Name.(create ~scope:n (name n'))))
          |> dedup_pairs
        in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Filter m (p, r) =
        super#visit_Filter None (p, r) ;
        let m = Option.value_exn m in
        let r_eqs = Meta.(find_exn r eq) in
        let eqs = Pred.eqs p @ r_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Select m (ps, r) =
        super#visit_Select None (ps, r) ;
        let m = Option.value_exn m in
        let eqs =
          Meta.(find_exn r eq)
          |> List.filter_map ~f:(fun ((n, n') as eq) ->
                 List.find_map ps
                   ~f:
                     (let open Name.O in
                     function
                     | Name n'' when n'' = n' || n'' = n -> Some eq
                     | As_pred (Name n'', s) when n'' = n -> Some (Name.create s, n')
                     | As_pred (Name n'', s) when n'' = n' -> Some (n, Name.create s)
                     | _ -> None) )
        in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_Join m p r1 r2 =
        super#visit_Join None p r1 r2 ;
        let m = Option.value_exn m in
        let r1_eqs = Meta.(find_exn r1 eq) in
        let r2_eqs = Meta.(find_exn r2 eq) in
        let eqs = Pred.eqs p @ r1_eqs @ r2_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_AList m (r1, r2) =
        super#visit_AList None (r1, r2) ;
        let m = Option.value_exn m in
        let r1_eqs = Meta.(find_exn r1 eq) in
        let r2_eqs = Meta.(find_exn r2 eq) in
        let eqs = r1_eqs @ r2_eqs |> dedup_pairs in
        Meta.Direct.set_m m Meta.eq eqs

      method! visit_t _ ({meta; _} as r) =
        Meta.(set_m r eq []) ;
        super#visit_t (Some meta) r
    end
  in
  visitor#visit_t None r

let select_kind l =
  if List.exists l ~f:(fun p -> Poly.(Pred.kind p = `Agg)) then `Agg else `Scalar

class ['a] stage_iter =
  object (self : 'a)
    inherit [_] iter

    method! visit_AList phase (rk, rv) =
      self#visit_t `Compile rk ;
      self#visit_t phase rv

    method! visit_AHashIdx phase h =
      List.iter h.hi_lookup ~f:(self#visit_pred phase) ;
      self#visit_t `Compile h.hi_keys ;
      self#visit_t phase h.hi_values

    method! visit_AOrderedIdx phase (rk, rv, {lookup_low; lookup_high; _}) =
      self#visit_pred phase lookup_low ;
      self#visit_pred phase lookup_high ;
      self#visit_t `Compile rk ;
      self#visit_t phase rv

    method! visit_AScalar _ p = self#visit_pred `Compile p
  end

exception Un_serial of string

let ops_serializable_exn r =
  let visitor =
    object
      inherit [_] stage_iter as super

      method! visit_t s r =
        super#visit_t s r ;
        match r.node with
        | Relation _ | GroupBy (_, _, _) | Join _ | OrderBy _ | Dedup _ ->
            raise (Un_serial "Cannot serialize: Bad operator in run-time position.")
        | _ -> ()
    end
  in
  visitor#visit_t `Run r

let names_serializable_exn r =
  let visitor =
    object
      inherit [_] stage_iter

      method! visit_Name s n =
        let this_ok = Poly.(Name.Meta.(find_exn n stage) = s) in
        if not this_ok then
          let stage = match s with `Compile -> "compile" | `Run -> "run" in
          raise
            (Un_serial
               (Format.asprintf "Cannot serialize: Found %a in %s time position."
                  Name.pp_with_stage n stage))
    end
  in
  visitor#visit_t `Run r

let is_serializeable_msg r =
  try ops_serializable_exn r ; names_serializable_exn r ; None
  with Un_serial msg -> Some msg

(** Return true if `r` is serializable. This function performs two checks:
     - `r` must not contain any compile time only operations in run time position.
     - Run-time names may only appear in run-time position and vice versa. *)
let is_serializeable r = Option.is_none (is_serializeable_msg r)

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
                   | _ -> None ) )
      | Filter (_, r) | AHashIdx {hi_values= r; _} -> annotate_orders r
      | DepJoin {d_lhs= r1; d_rhs= r2; _} | Join {r1; r2; _} ->
          annotate_orders r1 |> ignore ;
          annotate_orders r2 |> ignore ;
          []
      | GroupBy (_, _, r) | Dedup r ->
          annotate_orders r |> ignore ;
          []
      | Relation _ | AEmpty -> []
      | OrderBy {key; rel} ->
          annotate_orders rel |> ignore ;
          key
      | AScalar _ -> []
      | AList (r, r') ->
          let s' = schema_exn r' in
          let eq' = Meta.(find_exn r' eq) in
          annotate_orders r' |> ignore ;
          let open Name.O in
          annotate_orders r
          |> List.filter_map ~f:(function
               | Name n, dir ->
                   if List.mem ~equal:( = ) s' n then Some (Name n, dir)
                   else
                     List.find_map eq' ~f:(fun (n', n'') ->
                         if n = n' then Some (Name n'', dir)
                         else if n = n'' then Some (Name n', dir)
                         else None )
               | _ -> None )
      | ATuple (rs, Cross) -> List.map ~f:annotate_orders rs |> List.concat
      | ATuple (rs, (Zip | Concat)) ->
          List.iter ~f:(fun r -> annotate_orders r |> ignore) rs ;
          []
      | AOrderedIdx (r, _, _) ->
          schema_exn r |> List.map ~f:(fun n -> (Name n, Asc))
      | As _ -> []
    in
    Meta.set_m r Meta.order order ;
    order
  in
  annotate_eq r ;
  annotate_orders r |> ignore

let order_of r =
  annotate_orders r ;
  Meta.(find_exn r order)

let annotate_key_layouts r =
  let key_layout schema =
    match List.map schema ~f:(fun n -> scalar (Name n)) with
    | [] -> failwith "empty schema"
    | [x] -> x
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
        AHashIdx {h with hi_key_layout}

      method! visit_AOrderedIdx () ((x, y, ({oi_key_layout; _} as m)) as r) =
        let x = self#visit_t () x in
        let y = self#visit_t () y in
        match oi_key_layout with
        | Some _ -> AOrderedIdx r
        | None ->
            AOrderedIdx
              (x, y, {m with oi_key_layout= Some (key_layout (schema_exn x))})
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

      method! visit_As () _ _ r =
        Logs.warn (fun m -> m "Removing misplaced as: %a" pp_small r) ;
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
    List.map2_exn rk_schema h.hi_lookup ~f:(fun p1 p2 -> Binop (Eq, Name p1, p2))
    |> Pred.conjoin
  in
  let slist = rk_schema @ rv_schema |> List.map ~f:(fun n -> Name n) in
  dep_join h.hi_keys h.hi_scope (select slist (filter key_pred h.hi_values))

let ordered_idx_to_depjoin rk rv {lookup_low; lookup_high; _} =
  let scope = scope_exn rk in
  let rk_schema = schema_exn rk in
  let rv_schema = schema_exn rv in
  let key_pred =
    let rk_schema = schema_exn rk in
    match rk_schema with
    | [n] ->
        Pred.conjoin
          [Binop (Lt, lookup_low, Name n); Binop (Lt, Name n, lookup_high)]
    | _ -> failwith "Unexpected schema."
  in
  let slist = rk_schema @ rv_schema |> List.map ~f:(fun n -> Name n) in
  dep_join (strip_scope rk) scope (select slist (filter key_pred rv))

let ensure_alias r =
  let visitor =
    object (self)
      inherit [_] endo

      method! visit_Select () _ (ps, r) =
        Select
          ( List.map ps ~f:(fun p -> p |> self#visit_pred () |> Pred.ensure_alias)
          , self#visit_t () r )

      method! visit_GroupBy () _ ps k r =
        GroupBy
          ( List.map ps ~f:(fun p -> p |> self#visit_pred () |> Pred.ensure_alias)
          , k
          , self#visit_t () r )

      method! visit_AScalar () _ p =
        AScalar (self#visit_pred () p |> Pred.ensure_alias)
    end
  in
  visitor#visit_t () r
