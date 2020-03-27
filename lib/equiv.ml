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

let empty = Set.empty (module Eq)

let ( || ) l1 l2 = Set.union l1 l2

let ( && ) l1 l2 = Set.inter l1 l2

let of_list = Set.of_list (module Eq)

(** Return the set of equivalent attributes in the output of a query.

    Two attributes are equivalent in a relation if in every tuple of that
   relation, they have the same value. *)
let eqs_open (eqs : 'a annot -> Set.M(Eq).t) r : Set.M(Eq).t =
  match r with
  | Filter (p, r) -> Pred.eqs p |> of_list || eqs r
  | Select (ps, r) | GroupBy (ps, _, r) ->
      eqs r
      || List.filter_map ps ~f:(function
           | As_pred (Name n, s) -> Some (n, Name.create s)
           | _ -> None)
         |> of_list
  | Join { pred = p; r1; r2 } -> Pred.eqs p |> of_list || (eqs r1 && eqs r2)
  | Dedup r | OrderBy { rel = r; _ } | DepJoin { d_rhs = r; _ } -> eqs r
  | AList { l_keys = rk; l_values = rv; _ } -> eqs rk || eqs rv
  | AHashIdx h -> (
      match List.zip (Schema.schema h.hi_keys) h.hi_lookup with
      | Ok ls ->
          List.filter_map ls ~f:(fun (n, p) ->
              match p with Name n' -> Some (n, n') | _ -> None)
          |> of_list
      | Unequal_lengths -> empty )
  | ATuple (ts, Cross) ->
      List.map ts ~f:eqs |> List.reduce ~f:( || ) |> Option.value ~default:empty
  | _ -> empty

let annotate r = V.annotate eqs_open r

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

(** Two attributes are equivalent in a context if they can be substituted
   without changing the final relation. *)
module Context = struct
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
