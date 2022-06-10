open Core
open Castor
module P = Pred.Infix
module G = Egraph.AstEGraph

module EPred = struct
  module T = struct
    type t = Egraph.Id.t Ast.pred [@@deriving compare, sexp_of]
  end

  include T
  include Comparator.Make (T)
end

let return x f = f x

module Let_syntax = struct
  let bind x ~f = x f
  let map x ~f = bind ~f:(fun y -> return (f y)) x
end

let empty _ = ()
let concat_map func iter k = iter (fun x -> (func x) k)

module M = struct
  let mk_match m g r f = G.enodes g r |> Iter.filter_map m |> concat_map f

  let mk_match_any m g f =
    G.classes g
    |> concat_map (fun id ->
           G.enodes g id |> Iter.filter_map m |> concat_map (fun x -> f (id, x)))

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
  let dedup x = mk_match Ast.dedup_val x

  let tuple_concat x =
    mk_match
      (fun q ->
        Option.bind (Ast.atuple_val q) ~f:(function
          | ts, Concat -> Some ts
          | _ -> None))
      x

  let tuple_cross x =
    mk_match
      (fun q ->
        Option.bind (Ast.atuple_val q) ~f:(function
          | ts, Cross -> Some ts
          | _ -> None))
      x
end

module C = struct
  let mk_query c g x = G.add g (c x)

  let filter g p r =
    match p with `Bool true -> r | p -> G.add g (Filter (p, r))

  let filter_many g p r = filter g (P.and_ p) r
  let order_by g k r = G.add g (OrderBy { key = k; rel = r })
  let group_by g x y z = G.add g (GroupBy (x, y, z))
  let select g x y = G.add g (Select (x, y))
  let join g pred r1 r2 = G.add g (Ast.join { pred; r1; r2 })
  let dedup g = mk_query Ast.dedup g
  let list g = mk_query Ast.alist g
  let hash_idx g = mk_query Ast.ahashidx g
  let ordered_idx g = mk_query Ast.aorderedidx g
  let tuple g ts k = G.add g (ATuple (ts, k))
end

let to_annot = G.choose_exn
let of_annot = G.add_annot
