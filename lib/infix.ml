open! Core
open Ast
module A = Abslayout

let range = A.range

let select = A.select

let dep_join = A.dep_join

let join = A.join

let filter = A.filter

let group_by = A.group_by

let dedup = A.dedup

let order_by = A.order_by

let relation = A.relation

let empty = A.empty

let scalar = A.scalar

let list = A.list

let tuple = A.tuple

let hash_idx = A.hash_idx

let ordered_idx = A.ordered_idx

include Pred.Infix

let r n = Relation.{ r_name = n; r_schema = None } |> relation

let n s =
  let nm =
    match String.split s ~on:'.' with
    | [ f ] -> Name.create f
    | [ a; f ] -> Name.create ~scope:a f
    | _ -> failwith ("Unexpected name: " ^ s)
  in
  name nm

class ['self] constructors meta =
  object (self : 'self)
    method wrap x = { node = x; meta }

    method select a b = self#wrap @@ Select (a, b)

    method range a b = self#wrap @@ Range (a, b)

    method dep_join a b c =
      self#wrap @@ DepJoin { d_lhs = a; d_alias = b; d_rhs = c }

    method dep_join' d = self#dep_join d.d_lhs d.d_alias d.d_rhs

    method join a b c = self#wrap @@ Join { pred = a; r1 = b; r2 = c }

    method filter a b =
      self#wrap
      @@
      match Pred.kind a with
      | `Scalar -> Filter (a, b)
      | `Agg | `Window ->
          Error.of_string "Aggregates not allowed in filter." |> Error.raise

    method group_by a b c = self#wrap @@ GroupBy (a, b, c)

    method dedup a = self#wrap @@ Dedup a

    method order_by a b = self#wrap @@ OrderBy { key = a; rel = b }

    method relation r = self#wrap @@ Relation r

    method empty = self#wrap @@ AEmpty

    method scalar a = self#wrap @@ AScalar a

    method as_ a b = self#wrap @@ As (a, b)

    method list a b c = self#wrap @@ AList (self#as_ b a, c)

    method list' (a, b) = self#list a (A.scope_exn a) b

    method tuple a b = self#wrap @@ ATuple (a, b)

    method hash_idx ?key_layout a b c d =
      self#wrap
      @@ AHashIdx
           {
             hi_keys = a;
             hi_values = c;
             hi_scope = b;
             hi_lookup = d;
             hi_key_layout = key_layout;
           }

    method hash_idx' h =
      self#hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope h.hi_values
        h.hi_lookup

    method ordered_idx a b c d = self#wrap @@ AOrderedIdx (self#as_ b a, c, d)
  end
