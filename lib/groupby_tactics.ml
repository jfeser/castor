open Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Abslayout_db.Config.S

    val fresh : Fresh.t
  end
end

module Make (C : Config.S) = struct
  open C
  module Ops = Ops.Make (C)
  open Ops
  module M = Abslayout_db.Make (C)

  let elim_groupby r =
    M.annotate_schema r ;
    M.annotate_defs r ;
    annotate_free r ;
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name fresh "k%d" in
        let key_preds = List.map key ~f:(fun n -> Name n) in
        let filter_pred =
          List.map key ~f:(fun n ->
              Binop (Eq, Name n, Name (Name.copy n ~relation:(Some key_name)))
          )
          |> List.fold_left1_exn ~f:(fun acc p -> Binop (And, acc, p))
        in
        if Set.is_empty (free r) then
          (* Use precise version. *)
          Some
            (list
               (as_ key_name (dedup (select key_preds r)))
               (select ps (filter filter_pred r)))
        else
          let exception Failed of Error.t in
          let value_exn err = function
            | Some v -> v
            | None -> raise (Failed err)
          in
          (* Otherwise, if all grouping keys are from named relations,
             select all possible grouping keys. *)
          let defs = Meta.(find_exn r defs) in
          let rels = Hashtbl.create (module Abslayout) in
          let alias_map = aliases r in
          try
            (* Find the definition of each key. *)
            let key_defs =
              List.map key ~f:(fun n ->
                  List.find_map defs ~f:(fun (n', p) ->
                      Option.bind n' ~f:(fun n' ->
                          if Name.O.(n = n') then Some p else None ) )
                  |> value_exn
                       (Error.create "No definition found for key." n
                          [%sexp_of: Name.t]) )
            in
            (* Collect all the names in each definition. If they all come from
               base relations, then we can enumerate the keys. *)
            List.iter key_defs ~f:(fun p ->
                Set.iter (Pred.names p) ~f:(fun n ->
                    let r =
                      Name.rel n
                      |> value_exn
                           (Error.create
                              "Name does not come from base relation." n
                              [%sexp_of: Name.t])
                      |> Map.find alias_map
                      |> value_exn
                           (Error.create "Relation not found in alias table." n
                              [%sexp_of: Name.t])
                    in
                    Hashtbl.add_multi rels ~key:r ~data:(Name n) ) ) ;
            let key_rel =
              Hashtbl.to_alist rels
              |> List.map ~f:(fun (r, ns) -> dedup (select ns r))
              |> List.fold_left1_exn ~f:(join (Bool true))
            in
            Some
              (list
                 (as_ key_name (select key_defs key_rel))
                 (select ps (filter filter_pred r)))
          with Failed err ->
            Logs.info (fun m -> m "elim-groupby: %a" Error.pp err) ;
            None )
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby = of_func elim_groupby ~name:"elim-groupby"
end

module Test = struct
  module C = struct
    let conn = Db.create "postgresql:///tpch_1k"

    let verbose = false

    let validate = false

    let params =
      Set.of_list
        (module Name)
        [ Name.create ~type_:(StringT {nullable= false}) "param1"
        ; Name.create ~type_:(StringT {nullable= false}) "param2"
        ; Name.create ~type_:(StringT {nullable= false}) "param3" ]

    let param_ctx = Map.empty (module Name)

    let fresh = Fresh.create ()
  end

  module T = Make (C)
  open C
  open T
  open Ops

  let%expect_test "" =
    let r =
      of_string_exn
        {|
groupby([o_year,
         (sum((if (nation_name = param1) then volume else 0.0)) /
         sum(volume)) as mkt_share],
  [o_year],
  select([to_year(orders.o_orderdate) as o_year,
          (lineitem.l_extendedprice * (1 - lineitem.l_discount)) as volume,
          n2_name as nation_name],
    join((part.p_partkey = lineitem.l_partkey),
      join((supplier.s_suppkey = lineitem.l_suppkey),
        join((lineitem.l_orderkey = orders.o_orderkey),
          join((orders.o_custkey = customer.c_custkey),
            join((customer.c_nationkey = n1_nationkey),
              join((n1_regionkey = region.r_regionkey),
                select([nation.n_regionkey as n1_regionkey,
                        nation.n_nationkey as n1_nationkey],
                  nation),
                filter((region.r_name = param2), region)),
              customer),
            filter(((orders.o_orderdate >= date("1995-01-01")) &&
                   (orders.o_orderdate <= date("1996-12-31"))),
              orders)),
          lineitem),
        join((supplier.s_nationkey = n2_nationkey),
          select([nation.n_nationkey as n2_nationkey,
                  nation.n_name as n2_name],
            nation),
          supplier)),
      filter((part.p_type = param3), part))))
|}
    in
    let r = M.resolve ~params r in
    apply elim_groupby r |> Option.iter ~f:(Format.printf "%a@." Abslayout.pp);
    [%expect {|
      alist(select([to_year(orders.o_orderdate) as o_year],
              dedup(select([orders.o_orderdate], orders))) as k0,
        select([o_year,
                (sum((if (nation_name = param1) then volume else 0.0)) /
                sum(volume)) as mkt_share],
          filter((o_year = k0.o_year),
            select([to_year(orders.o_orderdate) as o_year,
                    (lineitem.l_extendedprice * (1 - lineitem.l_discount)) as volume,
                    n2_name as nation_name],
              join((part.p_partkey = lineitem.l_partkey),
                join((supplier.s_suppkey = lineitem.l_suppkey),
                  join((lineitem.l_orderkey = orders.o_orderkey),
                    join((orders.o_custkey = customer.c_custkey),
                      join((customer.c_nationkey = n1_nationkey),
                        join((n1_regionkey = region.r_regionkey),
                          select([nation.n_regionkey as n1_regionkey,
                                  nation.n_nationkey as n1_nationkey],
                            nation),
                          filter((region.r_name = param2), region)),
                        customer),
                      filter(((orders.o_orderdate >= date("1995-01-01")) &&
                             (orders.o_orderdate <= date("1996-12-31"))),
                        orders)),
                    lineitem),
                  join((supplier.s_nationkey = n2_nationkey),
                    select([nation.n_nationkey as n2_nationkey,
                            nation.n_name as n2_name],
                      nation),
                    supplier)),
                filter((part.p_type = param3), part)))))) |}]
end
