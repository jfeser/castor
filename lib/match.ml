open Ast

let to_dedup r = match r.node with Dedup r -> Some r | _ -> None

let to_join r =
  match r.node with Join { pred; r1; r2 } -> Some (pred, r1, r2) | _ -> None

let to_filter r = match r.node with Filter (p, r) -> Some (p, r) | _ -> None

let to_groupby r =
  match r.node with GroupBy (p, k, r) -> Some (p, k, r) | _ -> None

let to_select r = match r.node with Select (s, r) -> Some (s, r) | _ -> None

let to_list r = match r.node with AList l -> Some l | _ -> None

let to_depjoin r = match r.node with DepJoin x -> Some x | _ -> None
