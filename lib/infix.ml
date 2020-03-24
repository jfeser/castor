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
