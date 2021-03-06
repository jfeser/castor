open Ast
open Collections

(* Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
[@@@warning "-deprecated-7-17"]

type 'r pred = 'r Ast.pred =
  | Name of (Name.t[@opaque])
  | Int of (int[@opaque])
  | Fixed of (Fixed_point.t[@opaque])
  | Date of (Date.t[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null of (Prim_type.t option[@opaque])
  | Unop of (Unop.t[@opaque]) * 'r pred
  | Binop of (Binop.t[@opaque]) * 'r pred * 'r pred
  | As_pred of ('r pred * string)
  | Count
  | Row_number
  | Sum of 'r pred
  | Avg of 'r pred
  | Min of 'r pred
  | Max of 'r pred
  | If of 'r pred * 'r pred * 'r pred
  | First of 'r
  | Exists of 'r
  | Substring of 'r pred * 'r pred * 'r pred

and scope = string

and ('p, 'r) hash_idx = ('p, 'r) Ast.hash_idx = {
  hi_keys : 'r;
  hi_values : 'r;
  hi_scope : scope;
  hi_key_layout : 'r option;
  hi_lookup : 'p list;
}

and 'p bound = 'p * ([ `Open | `Closed ][@opaque])

and ('p, 'r) ordered_idx = ('p, 'r) Ast.ordered_idx = {
  oi_keys : 'r;
  oi_values : 'r;
  oi_scope : scope;
  oi_key_layout : 'r option;
  oi_lookup : ('p bound option * 'p bound option) list;
}

and ('p, 'r) list_ = ('p, 'r) Ast.list_ = {
  l_keys : 'r;
  l_values : 'r;
  l_scope : scope;
}

and 'r depjoin = 'r Ast.depjoin = { d_lhs : 'r; d_alias : scope; d_rhs : 'r }

and ('p, 'r) join = ('p, 'r) Ast.join = { pred : 'p; r1 : 'r; r2 : 'r }

and ('p, 'r) order_by = ('p, 'r) Ast.order_by = {
  key : ('p * (Ast.order[@opaque])) list;
  rel : 'r;
}

and ('p, 'r) query = ('p, 'r) Ast.query =
  | Select of ('p list * 'r)
  | Filter of ('p * 'r)
  | Join of ('p, 'r) join
  | DepJoin of 'r depjoin
  | GroupBy of ('p list * (Name.t[@opaque]) list * 'r)
  | OrderBy of ('p, 'r) order_by
  | Dedup of 'r
  | Relation of (Relation.t[@opaque])
  | Range of ('p * 'p)
  | AEmpty
  | AScalar of 'p
  | AList of ('p, 'r) list_
  | ATuple of ('r list * (Ast.tuple[@opaque]))
  | AHashIdx of ('p, 'r) hash_idx
  | AOrderedIdx of ('p, 'r) ordered_idx

and 'm annot = 'm Ast.annot = {
  node : ('m annot pred, 'm annot) query;
  meta : 'm;
}

and t = (meta[@opaque]) annot
[@@deriving
  visitors { variety = "endo"; name = "base_endo"; irregular = true },
    visitors { variety = "map"; name = "base_map"; irregular = true },
    visitors { variety = "iter"; name = "base_iter"; irregular = true },
    visitors { variety = "reduce"; name = "base_reduce"; irregular = true },
    visitors
      { variety = "mapreduce"; name = "base_mapreduce"; irregular = true }]

[@@@warning "+deprecated+7+17"]

module Map = struct
  let annot query { node; meta } = { node = query node; meta }

  let bound pred (p, b) = (pred p, b)

  let pred annot pred = function
    | ( Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null _ | Count
      | Row_number ) as p ->
        p
    | Unop (o, p) -> Unop (o, pred p)
    | Binop (o, p, p') -> Binop (o, pred p, pred p')
    | As_pred (p, s) -> As_pred (pred p, s)
    | Sum p -> Sum (pred p)
    | Avg p -> Avg (pred p)
    | Max p -> Max (pred p)
    | Min p -> Min (pred p)
    | If (p, p', p'') -> If (pred p, pred p', pred p'')
    | First q -> First (annot q)
    | Exists q -> Exists (annot q)
    | Substring (p, p', p'') -> Substring (pred p, pred p', pred p'')

  let ordered_idx query pred
      ({ oi_keys; oi_values; oi_key_layout; oi_lookup; _ } as o) =
    {
      o with
      oi_keys = query oi_keys;
      oi_values = query oi_values;
      oi_key_layout = Option.map oi_key_layout ~f:query;
      oi_lookup =
        List.map oi_lookup ~f:(fun (b, b') ->
            (Option.map ~f:(bound pred) b, Option.map ~f:(bound pred) b'));
    }

  let hash_idx query pred
      ({ hi_keys; hi_values; hi_key_layout; hi_lookup; _ } as h) =
    {
      h with
      hi_keys = query hi_keys;
      hi_values = query hi_values;
      hi_key_layout = Option.map hi_key_layout ~f:query;
      hi_lookup = List.map hi_lookup ~f:pred;
    }

  let list query pred l =
    { l with l_keys = query l.l_keys; l_values = query l.l_values }

  let query annot pred = function
    | Select (ps, q) -> Select (List.map ~f:pred ps, annot q)
    | Filter (p, q) -> Filter (pred p, annot q)
    | Join { pred = p; r1; r2 } ->
        Join { pred = pred p; r1 = annot r1; r2 = annot r2 }
    | DepJoin ({ d_lhs; d_rhs; _ } as d) ->
        DepJoin { d with d_lhs = annot d_lhs; d_rhs = annot d_rhs }
    | GroupBy (ps, ns, q) -> GroupBy (List.map ~f:pred ps, ns, annot q)
    | OrderBy { key; rel } ->
        OrderBy
          { key = List.map key ~f:(fun (p, o) -> (pred p, o)); rel = annot rel }
    | Dedup q -> Dedup (annot q)
    | (Relation _ | AEmpty) as q -> q
    | Range (p, p') -> Range (pred p, pred p')
    | AScalar p -> AScalar (pred p)
    | AList l -> AList (list annot pred l)
    | ATuple (qs, t) -> ATuple (List.map qs ~f:annot, t)
    | AHashIdx h -> AHashIdx (hash_idx annot pred h)
    | AOrderedIdx o -> AOrderedIdx (ordered_idx annot pred o)
end

let rec map_meta f { node; meta } =
  { node = map_meta_query f node; meta = f meta }

and map_meta_query f q = Map.query (map_meta f) (map_meta_pred f) q

and map_meta_pred f p = Map.pred (map_meta f) (map_meta_pred f) p

module Reduce = struct
  let rec list zero ( + ) f l =
    match l with [] -> zero | x :: xs -> f x + list zero ( + ) f xs

  let option zero f = function Some x -> f x | None -> zero

  let pred zero ( + ) annot pred = function
    | Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null _ | Count
    | Row_number ->
        zero
    | Unop (_, p) | As_pred (p, _) | Sum p | Avg p | Max p | Min p -> pred p
    | Binop (_, p, p') -> pred p + pred p'
    | If (p, p', p'') | Substring (p, p', p'') -> pred p + pred p' + pred p''
    | First q | Exists q -> annot q

  let query zero ( + ) annot pred = function
    | Relation _ | AEmpty -> zero
    | Select (ps, q) | GroupBy (ps, _, q) -> list zero ( + ) pred ps + annot q
    | Filter (p, q) -> pred p + annot q
    | Join { pred = p; r1; r2 } -> pred p + annot r1 + annot r2
    | DepJoin { d_lhs = q; d_rhs = q'; _ }
    | AList { l_keys = q; l_values = q'; _ } ->
        annot q + annot q'
    | OrderBy { key; rel } ->
        list zero ( + ) (fun (p, _) -> pred p) key + annot rel
    | Dedup q -> annot q
    | Range (p, p') -> pred p + pred p'
    | AScalar p -> pred p
    | ATuple (qs, _) -> list zero ( + ) annot qs
    | AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup; _ } ->
        annot hi_keys + annot hi_values
        + option zero annot hi_key_layout
        + list zero ( + ) pred hi_lookup
    | AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup; _ } ->
        annot oi_keys + annot oi_values
        + option zero annot oi_key_layout
        + list zero ( + )
            (fun (b, b') ->
              option zero (fun (p, _) -> pred p) b
              + option zero (fun (p, _) -> pred p) b')
            oi_lookup

  let annot zero ( + ) query meta { node; meta = m } = query node + meta m
end

module Stage_reduce = struct
  open Reduce

  let query zero ( + ) annot pred stage = function
    | AList { l_keys = q; l_values = q'; _ } ->
        annot `Compile q + annot stage q'
    | AScalar p -> pred `Compile p
    | AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup; _ } ->
        annot `Compile hi_keys
        + annot stage hi_values
        + option zero (annot stage) hi_key_layout
        + list zero ( + ) (pred stage) hi_lookup
    | AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup } ->
        annot `Compile oi_keys
        + annot stage oi_values
        + option zero (annot stage) oi_key_layout
        + list zero ( + )
            (fun (b, b') ->
              option zero (fun (p, _) -> pred stage p) b
              + option zero (fun (p, _) -> pred stage p) b')
            oi_lookup
    | q -> Reduce.query zero ( + ) (annot stage) (pred stage) q

  let pred zero ( + ) annot pred stage p =
    Reduce.pred zero ( + ) (annot stage) (pred stage) p

  let annot zero ( + ) query meta stage r =
    Reduce.annot zero ( + ) (query stage) (meta stage) r
end

module Iter = struct
  let pred annot pred = function
    | Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null _ | Count
    | Row_number ->
        ()
    | Unop (_, p) | As_pred (p, _) | Sum p | Avg p | Max p | Min p -> pred p
    | Binop (_, p, p') ->
        pred p;
        pred p'
    | If (p, p', p'') | Substring (p, p', p'') ->
        pred p;
        pred p';
        pred p''
    | First q | Exists q -> annot q

  let query annot pred = function
    | Relation _ | AEmpty -> ()
    | Select (ps, q) | GroupBy (ps, _, q) ->
        List.iter ~f:pred ps;
        annot q
    | Filter (p, q) ->
        pred p;
        annot q
    | Join { pred = p; r1; r2 } ->
        pred p;
        annot r1;
        annot r2
    | DepJoin { d_lhs = q; d_rhs = q'; _ }
    | AList { l_keys = q; l_values = q'; _ } ->
        annot q;
        annot q'
    | OrderBy { key; rel } ->
        List.iter ~f:(fun (p, _) -> pred p) key;
        annot rel
    | Dedup q -> annot q
    | Range (p, p') ->
        pred p;
        pred p'
    | AScalar p -> pred p
    | ATuple (qs, _) -> List.iter ~f:annot qs
    | AHashIdx { hi_keys; hi_values; hi_key_layout; hi_lookup; _ } ->
        annot hi_keys;
        annot hi_values;
        Option.iter ~f:annot hi_key_layout;
        List.iter ~f:pred hi_lookup
    | AOrderedIdx { oi_keys; oi_values; oi_key_layout; oi_lookup; _ } ->
        annot oi_keys;
        annot oi_values;
        Option.iter ~f:annot oi_key_layout;
        List.iter
          ~f:(fun (b, b') ->
            Option.iter ~f:(fun (p, _) -> pred p) b;
            Option.iter ~f:(fun (p, _) -> pred p) b')
          oi_lookup

  let annot query meta { node; meta = m } =
    query node;
    meta m
end

module Annotate = struct
  let rec annot f r =
    let node = query f r.node in
    { node; meta = f (fun r' -> r'.meta) node }

  and query f q = (Map.query (annot f) (pred f)) q

  and pred f p = Map.pred (annot f) (pred f) p
end

module Annotate_obj = struct
  let rec annot get set f r =
    let node = query get set f r.node in
    { node; meta = set r.meta @@ f (fun r' -> get r'.meta) node }

  and query get set f q = (Map.query (annot get set f) (pred get set f)) q

  and pred get set f p = Map.pred (annot get set f) (pred get set f) p
end

let rec annotate f r =
  let node = (Map.query (annotate f) (annotate_pred f)) r.node in
  { node; meta = f (fun r' -> r'.meta) node }

and annotate_pred f p = Map.pred (annotate f) (annotate_pred f) p

class virtual ['self] endo =
  object (self : 'self)
    inherit [_] base_endo

    method visit_'p = self#visit_pred

    method visit_'r = self#visit_t

    method visit_'m _ x = x
  end

class virtual ['self] map =
  object (self : 'self)
    inherit [_] base_map

    method visit_'p = self#visit_pred

    method visit_'r = self#visit_t

    method visit_'m _ x = x
  end

class virtual ['self] iter =
  object (self : 'self)
    inherit [_] base_iter

    method visit_'p = self#visit_pred

    method visit_'r = self#visit_t

    method visit_'m _ _ = ()
  end

class virtual ['self] reduce =
  object (self : 'self)
    inherit [_] base_reduce

    method visit_'p = self#visit_pred

    method visit_'r = self#visit_t

    method visit_'m _ _ = self#zero
  end

class virtual ['self] mapreduce =
  object (self : 'self)
    inherit [_] base_mapreduce

    method visit_'p = self#visit_pred

    method visit_'r = self#visit_t

    method visit_'m _ x = (x, self#zero)
  end

class ['a] names_visitor =
  object (self : 'a)
    inherit [_] reduce as super

    method zero = Set.empty (module Name)

    method plus = Set.union

    method! visit_Name () n = Set.singleton (module Name) n

    method! visit_pred () p =
      match p with
      | Exists _ | First _ -> self#zero
      | _ -> super#visit_pred () p
  end

class virtual ['m] runtime_subquery_visitor =
  object (self : 'a)
    inherit [_] iter as super

    method virtual visit_Subquery : 'm annot -> unit

    (* Don't annotate subqueries that run at compile time. *)
    method! visit_AScalar () _ = ()

    method! visit_AList () { l_values = r; _ } = super#visit_t () r

    method! visit_AHashIdx () { hi_values = r; _ } = super#visit_t () r

    method! visit_AOrderedIdx () { oi_values = r; _ } = super#visit_t () r

    method! visit_Exists () r =
      super#visit_t () r;
      self#visit_Subquery r

    method! visit_First () r =
      super#visit_t () r;
      self#visit_Subquery r
  end

class virtual ['self] runtime_subquery_map =
  object (self : 'self)
    inherit [_] map as super

    method virtual visit_Subquery : _

    (* Don't annotate subqueries that run at compile time. *)
    method! visit_AScalar _ x = AScalar x

    method! visit_AList acc ({ l_values = r; _ } as l) =
      AList { l with l_values = super#visit_t acc r }

    method! visit_AHashIdx acc ({ hi_values = r; _ } as h) =
      AHashIdx { h with hi_values = super#visit_t acc r }

    method! visit_AOrderedIdx acc ({ oi_values = r; _ } as o) =
      AOrderedIdx { o with oi_values = super#visit_t acc r }

    method! visit_Exists acc r =
      Exists (self#visit_Subquery (super#visit_t acc r))

    method! visit_First acc r =
      First (self#visit_Subquery (super#visit_t acc r))
  end
