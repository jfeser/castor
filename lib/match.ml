open Ast

let to_join r =
  match r.node with Join { pred; r1; r2 } -> Some (pred, r1, r2) | _ -> None

let to_filter r = match r.node with Filter (p, r) -> Some (p, r) | _ -> None
