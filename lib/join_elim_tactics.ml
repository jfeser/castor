open Base
open Castor
open Abslayout
open Collections

module Make (C : Ops.Config.S) = struct
  module O = Ops.Make (C)
  open O

  let fresh = Fresh.create ()

  let elim_join_nest r =
    match r.node with
    | Join {pred; r1; r2} -> Some (tuple [r1; filter pred r2] Cross)
    | _ -> None

  let elim_join_nest = of_func elim_join_nest ~name:"elim-join-nest"

  let elim_eq_filter r =
    match r.node with
    | Filter (p, r) ->
        let eqs, rest =
          conjuncts p
          |> List.partition_map ~f:(function
               | Binop (Eq, p1, p2) -> `Fst (p1, Fresh.name fresh "k%d", p2)
               | p -> `Snd p )
        in
        if List.length eqs = 0 then None
        else
          let select_list =
            List.map eqs ~f:(fun (p, k, _) -> As_pred (p, k))
          in
          let inner_filter_pred =
            List.map eqs ~f:(fun (p, k, _) ->
                Binop (Eq, Name (Name.create k), p) )
            |> and_
          in
          let key = List.map eqs ~f:(fun (_, _, p) -> p) in
          let outer_filter r =
            match rest with [] -> r | _ -> filter (and_ rest) r
          in
          Some
            (outer_filter
               (hash_idx
                  (dedup (select select_list r))
                  (filter inner_filter_pred r)
                  key))
    | _ -> None

  let elim_eq_filter = of_func elim_eq_filter ~name:"elim-eq-filter"

  let elim_join_hash = seq elim_join_nest (at_ elim_eq_filter last_child)
end
