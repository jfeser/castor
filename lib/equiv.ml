open Core
open Ast
module V = Visitors
include (val Log.make "castor.equiv")

module Eq = struct
  module T = struct
    type t = Name.t * Name.t [@@deriving compare, sexp]
  end

  include T
  include Comparator.Make (T)
end

type t = Set.M(Eq).t [@@deriving compare, sexp]
type meta = < eq : t >

let pp_meta fmt m = Fmt.pf fmt "%a" Sexp.pp_hum ([%sexp_of: t] m#eq)
let empty = Set.empty (module Eq)
let ( || ) l1 l2 = Set.union l1 l2
let ( && ) l1 l2 = Set.inter l1 l2
let of_list = Set.of_list (module Eq)

let names eq =
  Set.to_sequence eq
  |> Sequence.fold
       ~init:(Set.empty (module Name))
       ~f:(fun s (n, n') -> Set.add (Set.add s n') n)

let closure eq =
  let keys =
    names eq |> Set.to_sequence
    |> Sequence.map ~f:(fun n -> (n, Union_find.create n))
    |> Sequence.to_list
    |> Map.of_alist_exn (module Name)
  in
  Set.iter eq ~f:(fun (n, n') ->
      Union_find.union (Map.find_exn keys n) (Map.find_exn keys n'));
  Map.to_alist keys
  |> List.concat_map ~f:(fun (n, k) ->
         Map.to_alist keys
         |> List.concat_map ~f:(fun (n', k') ->
                if [%equal: Name.t] n n' then []
                else if Union_find.same_class k k' then [ (n, n'); (n', n) ]
                else []))
  |> Set.of_list (module Eq)

(** Return the set of equivalent attributes in the output of a query.

    Two attributes are equivalent in a relation if in every tuple of that
   relation, they have the same value. *)
let eqs_open (eqs : 'a annot -> Set.M(Eq).t) r : Set.M(Eq).t =
  match r with
  | Filter (p, r) -> Pred.eqs p |> of_list || eqs r
  | Select (ps, r) | GroupBy (ps, _, r) ->
      eqs r
      || List.filter_map ps ~f:(function
           | `Name n, s when String.(Name.name n <> s) -> Some (n, Name.create s)
           | _ -> None)
         |> of_list
  | Join { pred = p; r1; r2 } ->
      let e1 = eqs r1 and e2 = eqs r2 in
      let peqs = Pred.eqs p |> of_list in
      (* `If the equalities don't overlap, both sets continue to hold after the join. *)
      let jeqs =
        if Set.inter (names e1) (names e2) |> Set.is_empty then e1 || e2
        else e1 && e2
      in
      peqs || jeqs
  | Dedup r | OrderBy { rel = r; _ } | DepJoin { d_rhs = r; _ } -> eqs r
  | AList { l_keys = rk; l_values = rv; _ } -> eqs rk || eqs rv
  | AHashIdx h -> (
      match List.zip (Schema.schema h.hi_keys) h.hi_lookup with
      | Ok ls ->
          List.filter_map ls ~f:(fun (n, p) ->
              match p with `Name n' -> Some (n, n') | _ -> None)
          |> of_list
      | Unequal_lengths -> empty)
  | ATuple (ts, Cross) ->
      List.map ts ~f:eqs |> List.reduce ~f:( || ) |> Option.value ~default:empty
  | _ -> empty

let annotate r =
  V.Annotate_obj.annot
    (fun m -> m#eq)
    (fun m eq ->
      object
        method meta = m
        method eq = eq
      end)
    eqs_open r

let rec eqs r = closure @@ eqs_open eqs r.node

(** Two attributes are equivalent in a context if they can be substituted
   without changing the final relation. *)
module Context = struct
  type meta = < eqs : t >

  let rec annot eqs r =
    {
      node = query eqs r.node;
      meta =
        object
          method eqs = eqs
          method meta = r.meta
        end;
    }

  and query eqs = function
    | Filter (p, r) ->
        let eqs = Pred.eqs p |> of_list || eqs in
        Filter (pred eqs p, annot eqs r)
    | Join { pred = p; r1; r2 } ->
        let eqs = Pred.eqs p |> of_list || eqs in
        Join { pred = pred eqs p; r1 = annot eqs r1; r2 = annot eqs r2 }
    | q ->
        let eqs = empty in
        V.Map.query (annot eqs) (pred eqs) q

  and pred eqs p = V.Map.pred (annot eqs) (pred eqs) p

  let annotate r = annot empty r
end

