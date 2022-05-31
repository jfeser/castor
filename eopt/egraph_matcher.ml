open Castor
module G = Egraph.AstEGraph

let return x f = f x

module Let_syntax = struct
  let bind x ~f = x f
  let map x ~f = bind ~f:(fun y -> return (f y)) x
end

let empty _ = ()
let concat_map func iter k = iter (fun x -> (func x) k)

module M = struct
  let mk_match m g r f =
    G.enodes g r
    |> concat_map (function
         | Egraph.AstLang.Query q -> (
             match m q with Some x -> f x | None -> empty)
         | _ -> empty)

  let mk_match_any m g f =
    G.all_enodes g
    |> concat_map (function
         | Egraph.AstLang.Query q -> (
             match m q with Some x -> f x | None -> empty)
         | _ -> empty)

  let any_orderby x = mk_match_any Ast.orderby_val x
  let any_groupby x = mk_match_any Ast.groupby_val x
  let any_filter x = mk_match_any Ast.filter_val x
  let any_select x = mk_match_any Ast.select_val x
  let any_join x = mk_match_any Ast.join_val x
  let any_dedup x = mk_match_any Ast.dedup_val x
  let any_list x = mk_match_any Ast.alist_val x
  let any_hashidx x = mk_match_any Ast.ahashidx_val x
  let any_orderedidx x = mk_match_any Ast.aorderedidx_val x
  let orderby x = mk_match Ast.orderby_val x
  let filter x = mk_match Ast.filter_val x
end

module C = struct
  let mk_query c g x = G.add g (Query (c x))
  let filter g p r = G.add g (Query (Filter (p, r)))
  let order_by g k r = G.add g (Query (OrderBy { key = k; rel = r }))
  let group_by g x y z = G.add g (Query (GroupBy (x, y, z)))
  let select g x y = G.add g (Query (Select (x, y)))
  let join g pred r1 r2 = G.add g (Query (Ast.join { pred; r1; r2 }))
  let dedup g = mk_query Ast.dedup g
  let list g = mk_query Ast.alist g
  let hash_idx g = mk_query Ast.ahashidx g
  let ordered_idx g = mk_query Ast.aorderedidx g
  let binop g x y z = G.add g (Pred (`Binop (x, y, z)))

  let rec and_ g = function
    | [] -> G.add g (Pred (`Bool true))
    | [ x ] -> x
    | x :: xs -> binop g And x (and_ g xs)
end

let to_annot g r =
  match G.choose_exn g r with `Annot r -> r | _ -> assert false

let to_pred g r = match G.choose_exn g r with `Pred r -> r | _ -> assert false
let of_pred = G.add_pred
