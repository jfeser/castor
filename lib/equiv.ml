open Ast
open Abslayout_visitors
open Schema

type t = (Name.t * Name.t) list [@@deriving compare, sexp]

let dedup_pairs = List.dedup_and_sort ~compare:[%compare: Name.t * Name.t]

(** Return the set of equivalent attributes in the output of a query. *)
let eqs_open eqs r =
  let ret =
    match r with
    | As (scope, r) ->
        schema r |> List.map ~f:(fun n -> (n, Name.(scoped scope n)))
    | Filter (p, r) -> Pred.eqs p @ eqs r
    | Select (ps, r) | GroupBy (ps, _, r) ->
        let eqs = eqs r in
        eqs
        @ List.filter_map ps ~f:(function
            | As_pred (Name n, s) -> Some (n, Name.create s)
            | _ -> None)
    | Join { pred = p; r1; r2 } -> Pred.eqs p @ eqs r1 @ eqs r2
    | Dedup r | OrderBy { rel = r; _ } | DepJoin { d_rhs = r; _ } | AList (_, r)
      ->
        eqs r
    | _ -> []
  in
  dedup_pairs ret

let annotate r = annotate eqs_open r

let rec eqs r = eqs_open eqs r.node

let is_equiv eqs n n' =
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
