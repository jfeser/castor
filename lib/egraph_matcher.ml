module Make (G : sig
  val g : Egraph.AstEGraph.t
end)
() =
struct
  open G

  module Let_syntax = struct
    let bind x ~f = x f
  end

  let return x f = f x
  let empty _ = ()
  let concat_map func iter k = iter (fun x -> (func x) k)

  module G = Egraph.AstEGraph

  module Match = struct
    let mk_match m r f =
      G.enodes g r
      |> concat_map (function
           | Egraph.AstLang.Query q -> (
               match m q with Some x -> f x | None -> empty)
           | _ -> empty)

    let orderby x = mk_match Ast.orderby_val x
    let filter x = mk_match Ast.filter_val x
  end

  module Construct = struct
    let filter p r = G.add g (Query (Filter (p, r)))
    let order_by k r = G.add g (Query (OrderBy { key = k; rel = r }))
  end

  let to_annot r =
    match G.choose_exn g r with `Annot r -> r | _ -> assert false
end
