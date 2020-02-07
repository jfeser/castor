open! Core
open Ast
open Collections

(* Visitors doesn't use the special method override syntax that warning 7 checks
   for. *)
[@@@warning "-7"]

type pred = Ast.pred =
  | Name of (Name.t[@opaque])
  | Int of (int[@opaque])
  | Fixed of (Fixed_point.t[@opaque])
  | Date of (Date.t[@opaque])
  | Bool of (bool[@opaque])
  | String of (string[@opaque])
  | Null of (Prim_type.t option[@opaque])
  | Unop of (Unop.t[@opaque]) * pred
  | Binop of (Binop.t[@opaque]) * pred * pred
  | As_pred of (pred * string)
  | Count
  | Row_number
  | Sum of pred
  | Avg of pred
  | Min of pred
  | Max of pred
  | If of pred * pred * pred
  | First of t
  | Exists of t
  | Substring of pred * pred * pred

and scope = string

and hash_idx = Ast.hash_idx = {
  hi_keys : t;
  hi_values : t;
  hi_scope : scope;
  hi_key_layout : t option; [@opaque]
  hi_lookup : pred list;
}

and bound = pred * ([ `Open | `Closed ][@opaque])

and ordered_idx = Ast.ordered_idx = {
  oi_key_layout : t option;
  oi_lookup : (bound option * bound option) list;
}

and depjoin = Ast.depjoin = { d_lhs : t; d_alias : scope; d_rhs : t }

and join = Ast.join = { pred : pred; r1 : t; r2 : t }

and order_by = Ast.order_by = {
  key : (pred * (Ast.order[@opaque])) list;
  rel : t;
}

and t = Ast.t = { node : node; meta : Meta.t [@opaque] }

and node = Ast.node =
  | Select of (pred list * t)
  | Filter of (pred * t)
  | Join of join
  | DepJoin of depjoin
  | GroupBy of (pred list * (Name.t[@opaque]) list * t)
  | OrderBy of order_by
  | Dedup of t
  | Relation of (Relation.t[@opaque])
  | Range of pred * pred
  | AEmpty
  | AScalar of pred
  | AList of (t * t)
  | ATuple of (t list * (Ast.tuple[@opaque]))
  | AHashIdx of hash_idx
  | AOrderedIdx of (t * t * ordered_idx)
  | As of scope * t
[@@deriving
  visitors { variety = "endo" },
    visitors { variety = "map" },
    visitors { variety = "iter" },
    visitors { variety = "reduce" },
    visitors { variety = "fold"; ancestors = [ "map" ] },
    visitors { variety = "mapreduce" }]

[@@@warning "+7"]

let names_visitor =
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
