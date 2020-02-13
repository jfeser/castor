open Ast
open Collections

(* Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
[@@@warning "-7-17"]

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
  oi_key_layout : 'r option;
  oi_lookup : ('p bound option * 'p bound option) list;
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
  | Range of 'p * 'p
  | AEmpty
  | AScalar of 'p
  | AList of ('r * 'r)
  | ATuple of ('r list * (Ast.tuple[@opaque]))
  | AHashIdx of ('p, 'r) hash_idx
  | AOrderedIdx of ('r * 'r * ('p, 'r) ordered_idx)
  | As of scope * 'r

and 'm annot = 'm Ast.annot = {
  node : ('m annot pred, 'm annot) query;
  meta : 'm;
}

and t = (Meta.t[@opaque]) annot
[@@deriving
  visitors { variety = "endo"; name = "base_endo"; irregular = true },
    visitors { variety = "map"; name = "base_map"; irregular = true },
    visitors { variety = "iter"; name = "base_iter"; irregular = true },
    visitors { variety = "reduce"; name = "base_reduce"; irregular = true },
    visitors
      { variety = "mapreduce"; name = "base_mapreduce"; irregular = true }]

[@@@warning "+7+17"]

let map_annot map_query { node; meta } = { node = map_query node; meta }

let map_bound map_pred (p, b) = (map_pred p, b)

let map_pred map_query map_pred = function
  | ( Name _ | Int _ | Fixed _ | Date _ | Bool _ | String _ | Null _ | Count
    | Row_number ) as p ->
      p
  | Unop (o, p) -> Unop (o, map_pred p)
  | Binop (o, p, p') -> Binop (o, map_pred p, map_pred p')
  | As_pred (p, s) -> As_pred (map_pred p, s)
  | Sum p -> Sum (map_pred p)
  | Avg p -> Avg (map_pred p)
  | Max p -> Max (map_pred p)
  | Min p -> Min (map_pred p)
  | If (p, p', p'') -> If (map_pred p, map_pred p', map_pred p'')
  | First q -> First (map_query q)
  | Exists q -> Exists (map_query q)
  | Substring (p, p', p'') -> Substring (map_pred p, map_pred p', map_pred p'')

let map_query map_query map_pred = function
  | Select (ps, q) -> Select (List.map ~f:map_pred ps, map_query q)
  | Filter (p, q) -> Filter (map_pred p, map_query q)
  | Join { pred; r1; r2 } ->
      Join { pred = map_pred pred; r1 = map_query r1; r2 = map_query r2 }
  | DepJoin ({ d_lhs; d_rhs; _ } as d) ->
      DepJoin { d with d_lhs = map_query d_lhs; d_rhs = map_query d_rhs }
  | GroupBy (ps, ns, q) -> GroupBy (List.map ~f:map_pred ps, ns, map_query q)
  | OrderBy { key; rel } ->
      OrderBy
        {
          key = List.map key ~f:(fun (p, o) -> (map_pred p, o));
          rel = map_query rel;
        }
  | Dedup q -> Dedup (map_query q)
  | (Relation _ | AEmpty) as q -> q
  | Range (p, p') -> Range (map_pred p, map_pred p')
  | AScalar p -> AScalar (map_pred p)
  | AList (q, q') -> AList (map_query q, map_query q')
  | ATuple (qs, t) -> ATuple (List.map qs ~f:map_query, t)
  | AHashIdx ({ hi_keys; hi_values; hi_key_layout; hi_lookup; _ } as h) ->
      AHashIdx
        {
          h with
          hi_keys = map_query hi_keys;
          hi_values = map_query hi_values;
          hi_key_layout = Option.map hi_key_layout ~f:map_query;
          hi_lookup = List.map hi_lookup ~f:map_pred;
        }
  | AOrderedIdx (q, q', { oi_key_layout; oi_lookup }) ->
      AOrderedIdx
        ( map_query q,
          map_query q',
          {
            oi_key_layout = Option.map oi_key_layout ~f:map_query;
            oi_lookup =
              List.map oi_lookup ~f:(fun (b, b') ->
                  ( Option.map ~f:(map_bound map_pred) b,
                    Option.map ~f:(map_bound map_pred) b' ));
          } )
  | As (s, q) -> As (s, map_query q)

let rec map_meta f { node; meta } =
  { node = map_query (map_meta f) (map_meta_pred f) node; meta = f meta }

and map_meta_pred f p = map_pred (map_meta f) (map_meta_pred f) p

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

class virtual runtime_subquery_visitor =
  object (self : 'a)
    inherit [_] iter as super

    method virtual visit_Subquery : t -> unit

    (* Don't annotate subqueries that run at compile time. *)
    method! visit_AScalar () _ = ()

    method! visit_AList () (_, r) = super#visit_t () r

    method! visit_AHashIdx () { hi_values = r; _ } = super#visit_t () r

    method! visit_AOrderedIdx () (_, r, _) = super#visit_t () r

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

    method! visit_AList acc (x, r) = AList (x, super#visit_t acc r)

    method! visit_AHashIdx acc ({ hi_values = r; _ } as h) =
      AHashIdx { h with hi_values = super#visit_t acc r }

    method! visit_AOrderedIdx acc (x, r, y) =
      AOrderedIdx (x, super#visit_t acc r, y)

    method! visit_Exists acc r =
      Exists (self#visit_Subquery (super#visit_t acc r))

    method! visit_First acc r =
      First (self#visit_Subquery (super#visit_t acc r))
  end
