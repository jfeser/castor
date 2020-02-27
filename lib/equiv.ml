open Ast
open Abslayout_visitors

module Eq = struct
  module T = struct
    type t = Name.t * Name.t [@@deriving compare, sexp]
  end

  include T
  include Comparator.Make (T)
end

type t = Set.M(Eq).t [@@deriving compare, sexp]

let ( || ) l1 l2 = Set.union l1 l2

let ( && ) l1 l2 = Set.inter l1 l2

(** Return the set of equivalent attributes in the output of a query.

    Two attributes are equivalent in a relation if in every tuple of that
   relation, they have the same value. *)
let eqs_open (eqs : 'a annot -> Set.M(Eq).t) r : Set.M(Eq).t =
  match r with
  | As (scope, r) ->
      eqs r
      || Schema.schema r
         |> List.map ~f:(fun n -> (n, Name.(scoped scope n)))
         |> Set.of_list (module Eq)
  | Filter (p, r) -> Pred.eqs p |> Set.of_list (module Eq) || eqs r
  | Select (ps, r) | GroupBy (ps, _, r) ->
      eqs r
      || List.filter_map ps ~f:(function
           | As_pred (Name n, s) -> Some (n, Name.create s)
           | _ -> None)
         |> Set.of_list (module Eq)
  | Join { pred = p; r1; r2 } ->
      Pred.eqs p |> Set.of_list (module Eq) || (eqs r1 && eqs r2)
  | Dedup r | OrderBy { rel = r; _ } | DepJoin { d_rhs = r; _ } | AList (_, r)
    ->
      eqs r
  | _ -> Set.empty (module Eq)

let annotate r = annotate eqs_open r

let rec eqs r = eqs_open eqs r.node

let is_equiv eqs n n' =
  let eqs = Set.to_list eqs in
  let classes =
    [ n; n' ] @ List.concat_map eqs ~f:(fun (n, n') -> [ n; n' ])
    |> List.dedup_and_sort ~compare:[%compare: Name.t]
    |> List.map ~f:(fun n -> (n, Union_find.create n))
    |> Map.of_alist_exn (module Name)
  in
  List.iter eqs ~f:(fun (n, n') ->
      Union_find.union (Map.find_exn classes n) (Map.find_exn classes n'));
  Union_find.same_class (Map.find_exn classes n) (Map.find_exn classes n')

let all_equiv eqs n =
  let eqs = Set.to_list eqs in
  let classes =
    n :: List.concat_map eqs ~f:(fun (n, n') -> [ n; n' ])
    |> List.dedup_and_sort ~compare:[%compare: Name.t]
    |> List.map ~f:(fun n -> (n, Union_find.create n))
    |> Map.of_alist_exn (module Name)
  in
  List.iter eqs ~f:(fun (n, n') ->
      Union_find.union (Map.find_exn classes n) (Map.find_exn classes n'));
  let c = Map.find_exn classes n in
  Map.to_alist classes
  |> List.filter_map ~f:(fun (n', c') ->
         if Union_find.same_class c c' then Some n' else None)
  |> Set.of_list (module Name)

module Context = struct end
(** Two attributes are equivalent in a context if they can be substituted without changing the final relation. *)
