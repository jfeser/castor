open Core
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module M = Abslayout_db.Make (C)

  let test = Logs.Src.create ~doc:"Source for testing project." "project-test"

  let project_defs refcnt ps =
    List.filter ps ~f:(fun p ->
        match pred_to_name p with
        | None ->
            (* Filter out definitions that have no name *)
            false
        | Some n -> (
          (* Filter out definitions that are never referenced. *)
          match Map.(find refcnt n) with
          | Some c -> c > 0
          | None ->
              (* Be conservative if refcount is missing. *)
              true ) )

  let all_unref refcnt schema =
    List.for_all schema ~f:(fun n ->
        match Map.(find refcnt n) with Some c -> c = 0 | None -> false )

  let pp_with_refcount, _ =
    mk_pp
      ~pp_meta:(fun fmt meta ->
        let open Format in
        match Univ_map.find meta M.refcnt with
        | Some r ->
            fprintf fmt "@[<hv 2>{" ;
            Map.iteri r ~f:(fun ~key:n ~data:c ->
                if c > 0 then fprintf fmt "%a=%d,@ " Name.pp n c ) ;
            fprintf fmt "}@]"
        | None -> () )
      ()

  let project_visitor =
    object (self : 'a)
      inherit [_] map as super

      method! visit_t count r =
        let open Option.Let_syntax in
        let r' =
          let%bind refcnt = Univ_map.(find !(r.meta) M.refcnt) in
          let%map schema = Meta.(find r Meta.schema) in
          if (not count) && all_unref refcnt schema then empty
          else
            match r.node with
            | AList ({node= AEmpty; _}, _)
             |AList (_, {node= AEmpty; _})
             |AHashIdx ({node= AEmpty; _}, _, _)
             |AHashIdx (_, {node= AEmpty; _}, _)
             |AOrderedIdx ({node= AEmpty; _}, _, _)
             |AOrderedIdx (_, {node= AEmpty; _}, _)
             |Select (_, {node= AEmpty; _})
             |Filter (_, {node= AEmpty; _})
             |Dedup {node= AEmpty; _}
             |GroupBy (_, _, {node= AEmpty; _})
             |OrderBy {rel= {node= AEmpty; _}; _}
             |Join {r1= {node= AEmpty; _}; _}
             |Join {r2= {node= AEmpty; _}; _} ->
                empty
            | Select (ps, r) ->
                let count =
                  count || List.exists ps ~f:(function Count -> true | _ -> false)
                in
                select (project_defs refcnt ps) (self#visit_t count r)
            | Dedup r -> dedup (self#visit_t false r)
            | GroupBy (ps, ns, r) ->
                let count =
                  count || List.exists ps ~f:(function Count -> true | _ -> false)
                in
                group_by (project_defs refcnt ps) ns (self#visit_t count r)
            | AScalar p -> (
              match project_defs refcnt [p] with
              | [] -> if count then scalar Null else empty
              | [p] -> scalar p
              | _ -> assert false )
            | ATuple ([], _) -> empty
            | ATuple ([r], _) -> self#visit_t count r
            | ATuple (rs, Concat) ->
                let rs = List.map rs ~f:(self#visit_t count) in
                let rs = List.filter rs ~f:(fun r -> r.node <> AEmpty) in
                tuple rs Concat
            | ATuple (rs, Cross) ->
                if
                  List.exists rs ~f:(fun r ->
                      match r.node with AEmpty -> true | _ -> false )
                then empty
                else
                  let rs =
                    (* Remove unreferenced parts of the tuple. *)
                    List.filter rs ~f:(fun r ->
                        let is_unref =
                          all_unref
                            Univ_map.(find_exn !(r.meta) M.refcnt)
                            Meta.(find_exn r schema)
                        in
                        let is_scalar =
                          match r.node with AScalar _ -> true | _ -> false
                        in
                        (* If the count matters, then we can only remove
                           unreferenced scalars. *)
                        let should_remove =
                          (count && is_unref && is_scalar)
                          || (* Otherwise we can remove anything unreferenced. *)
                             ((not count) && is_unref)
                        in
                        if should_remove then
                          Logs.debug ~src:test (fun m ->
                              m "Removing tuple element %a." pp_with_refcount r ) ;
                        not should_remove )
                  in
                  let rs =
                    (* We care about the count here (or at least the difference
                       between 1 record and none). *)
                    List.map rs ~f:(self#visit_t true)
                  in
                  let rs =
                    if count && List.length rs = 0 then [scalar Null] else rs
                  in
                  tuple rs Cross
            | Join {r1; r2; pred} ->
                (* If one side of a join is unused then the join can be dropped. *)
                let r1_unref = all_unref refcnt Meta.(find_exn r1 schema) in
                let r2_unref = all_unref refcnt Meta.(find_exn r2 schema) in
                let r1 = self#visit_t count r1 in
                let r2 = self#visit_t count r2 in
                if count then join pred r1 r2
                else if r1_unref then r2
                else if r2_unref then r1
                else join pred r1 r2
            | AHashIdx (rk, rv, m) ->
                hash_idx' (self#visit_t false rk) (self#visit_t count rv) m
            | AOrderedIdx (rk, rv, m) ->
                ordered_idx (self#visit_t false rk) (self#visit_t count rv) m
            | _ -> super#visit_t count r
        in
        match r' with Some r' -> r' | None -> r
    end

  let project_once = project_visitor#visit_t true

  let project ?(params = Set.empty (module Name)) r =
    let rec loop r =
      let r' = M.resolve r ~params in
      M.annotate_schema r' ;
      Logs.debug ~src:test (fun m -> m "pre %a@." pp_with_refcount r') ;
      let r' = project_once r' in
      Logs.debug ~src:test (fun m -> m "post %a@." pp_with_refcount r') ;
      if Abslayout.O.(r = r') then r' else loop r'
    in
    loop r
end

module Test = struct
  module T = Make (struct
    let conn = Db.create "postgresql:///tpch_1k"
  end)

  open T

  let%expect_test "" =
    let r =
      {|
      select([lineitem.l_receiptdate],
                         alist(join((orders.o_orderkey = lineitem.l_orderkey),
                                 lineitem,
                                 orders),
                           atuple([ascalar(lineitem.l_orderkey),
                                   ascalar(lineitem.l_commitdate),
                                   ascalar(lineitem.l_receiptdate),
                                   ascalar(orders.o_comment)],
                             cross)))
    |}
      |> of_string_exn
    in
    project r |> Format.printf "%a@." pp ;
    [%expect
      {|
      select([lineitem.l_receiptdate],
        alist(join((orders.o_orderkey = lineitem.l_orderkey), lineitem, orders),
          ascalar(lineitem.l_receiptdate))) |}]

  let%expect_test "" =
    let r =
      of_string_exn
        {|
alist(orderby([k0.l_shipmode],
         select([lineitem.l_shipmode],
           dedup(select([lineitem.l_shipmode], lineitem))) as k0),
   select([lineitem.l_shipmode,
           sum(agg2) as high_line_count,
           sum(agg3) as low_line_count],
     aorderedidx(dedup(
                   select([lineitem.l_receiptdate as k1],
                     dedup(
                       select([lineitem.l_receiptdate],
                         alist(join((orders.o_orderkey = lineitem.l_orderkey),
                                 lineitem,
                                 orders),
                           atuple([ascalar(lineitem.l_commitdate),
                                   ascalar(lineitem.l_receiptdate)],
                             cross)))))),
       filter((count4 > 0),
         select([count() as count4,
                 sum((if (not((orders.o_orderpriority = "1-URGENT")) &&
                         not((orders.o_orderpriority = "2-HIGH"))) then 1 else 0)) as agg3,
                 sum((if ((orders.o_orderpriority = "1-URGENT") ||
                         (orders.o_orderpriority = "2-HIGH")) then 1 else 0)) as agg2,
                 lineitem.l_shipmode],
           alist(filter(((lineitem.l_shipmode = k0.l_shipmode) &&
                        ((lineitem.l_commitdate < lineitem.l_receiptdate) &&
                        ((lineitem.l_shipdate < lineitem.l_commitdate) &&
                        (k1 = lineitem.l_receiptdate)))),
                   join((orders.o_orderkey = lineitem.l_orderkey),
                     lineitem,
                     orders)),
             filter(((lineitem.l_shipmode = param1) ||
                    (lineitem.l_shipmode = param2)),
               atuple([ascalar(lineitem.l_shipmode),
                       ascalar(orders.o_orderpriority)],
                 cross))))),
       (param3 + day(1)),
       ((param3 + year(1)) + day(1)))))
|}
    in
    project
      ~params:
        (Set.of_list
           (module Name)
           [ Name.create ~type_:(StringT {nullable= false}) "param1"
           ; Name.create ~type_:(StringT {nullable= false}) "param2"
           ; Name.create ~type_:(DateT {nullable= false}) "param3" ])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      alist(orderby([k0.l_shipmode],
              select([lineitem.l_shipmode],
                dedup(select([lineitem.l_shipmode], lineitem))) as k0),
        select([lineitem.l_shipmode,
                sum(agg2) as high_line_count,
                sum(agg3) as low_line_count],
          aorderedidx(dedup(
                        select([lineitem.l_receiptdate as k1],
                          dedup(
                            select([lineitem.l_receiptdate],
                              alist(lineitem, ascalar(lineitem.l_receiptdate)))))),
            filter((count4 > 0),
              select([count() as count4,
                      sum((if (not((orders.o_orderpriority = "1-URGENT")) &&
                              not((orders.o_orderpriority = "2-HIGH"))) then 1 else 0)) as agg3,
                      sum((if ((orders.o_orderpriority = "1-URGENT") ||
                              (orders.o_orderpriority = "2-HIGH")) then 1 else 0)) as agg2,
                      lineitem.l_shipmode],
                alist(filter(((lineitem.l_shipmode = k0.l_shipmode) &&
                             ((lineitem.l_commitdate < lineitem.l_receiptdate) &&
                             ((lineitem.l_shipdate < lineitem.l_commitdate) &&
                             (k1 = lineitem.l_receiptdate)))),
                        join((orders.o_orderkey = lineitem.l_orderkey),
                          lineitem,
                          orders)),
                  filter(((lineitem.l_shipmode = param1) ||
                         (lineitem.l_shipmode = param2)),
                    atuple([ascalar(lineitem.l_shipmode),
                            ascalar(orders.o_orderpriority)],
                      cross))))),
            (param3 + day(1)),
            ((param3 + year(1)) + day(1))))) |}]

  let%expect_test _ =
    let r =
      of_string_exn
        {|
select([nation.n_name, revenue],
   alist(select([nation.n_name], dedup(select([nation.n_name], nation))) as k0,
     select([nation.n_name, sum(agg3) as revenue],
       aorderedidx(dedup(
                     select([orders.o_orderdate as k2],
                       dedup(select([orders.o_orderdate], orders)))),
         filter((count4 > 0),
           select([count() as count4,
                   sum((lineitem.l_extendedprice * (1 - lineitem.l_discount))) as agg3,
                   nation.n_name,
                   nation.n_regionkey,
                   nation.n_comment],
             ahashidx(dedup(
                        select([region.r_name as k1],
                          atuple([alist(region,
                                    atuple([ascalar(region.r_regionkey),
                                            ascalar(region.r_name),
                                            ascalar(region.r_comment)],
                                      cross)),
                                  filter((nation.n_regionkey =
                                         region.r_regionkey),
                                    alist(join((supplier.s_nationkey =
                                               nation.n_nationkey),
                                            join(((lineitem.l_suppkey =
                                                  supplier.s_suppkey) &&
                                                 (customer.c_nationkey =
                                                 supplier.s_nationkey)),
                                              join((lineitem.l_orderkey =
                                                   orders.o_orderkey),
                                                join((customer.c_custkey =
                                                     orders.o_custkey),
                                                  customer,
                                                  orders),
                                                lineitem),
                                              supplier),
                                            nation),
                                      atuple([ascalar(nation.n_regionkey),
                                              ascalar(nation.n_comment)],
                                        cross)))],
                            cross))),
               atuple([alist(filter((k1 = region.r_name), region),
                         atuple([ascalar(region.r_regionkey),
                                 ascalar(region.r_name),
                                 ascalar(region.r_comment)],
                           cross)),
                       filter((nation.n_regionkey = region.r_regionkey),
                         alist(filter(((nation.n_name = k0.n_name) &&
                                      (k2 = orders.o_orderdate)),
                                 join((supplier.s_nationkey =
                                      nation.n_nationkey),
                                   join(((lineitem.l_suppkey =
                                         supplier.s_suppkey) &&
                                        (customer.c_nationkey =
                                        supplier.s_nationkey)),
                                     join((lineitem.l_orderkey =
                                          orders.o_orderkey),
                                       join((customer.c_custkey =
                                            orders.o_custkey),
                                         customer,
                                         orders),
                                       lineitem),
                                     supplier),
                                   nation)),
                           atuple([ascalar(lineitem.l_extendedprice),
                                   ascalar(lineitem.l_discount),
                                   ascalar(lineitem.l_tax),
                                   ascalar(nation.n_nationkey),
                                   ascalar(nation.n_name),
                                   ascalar(nation.n_regionkey),
                                   ascalar(nation.n_comment)],
                             cross)))],
                 cross),
               param0))),
         (param1 + day(1)),
         ((param1 + year(1)) + day(1))))))|}
    in
    project
      ~params:
        (Set.of_list
           (module Name)
           [ Name.create ~type_:(StringT {nullable= false}) "param0"
           ; Name.create ~type_:(DateT {nullable= false}) "param1" ])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      select([nation.n_name, revenue],
        alist(select([nation.n_name], dedup(select([nation.n_name], nation))) as k0,
          select([nation.n_name, sum(agg3) as revenue],
            aorderedidx(dedup(
                          select([orders.o_orderdate as k2],
                            dedup(select([orders.o_orderdate], orders)))),
              filter((count4 > 0),
                select([count() as count4,
                        sum((lineitem.l_extendedprice * (1 - lineitem.l_discount))) as agg3,
                        nation.n_name],
                  ahashidx(dedup(
                             select([region.r_name as k1],
                               alist(region, ascalar(region.r_name)))),
                    atuple([alist(filter((k1 = region.r_name), region),
                              ascalar(region.r_regionkey)),
                            filter((nation.n_regionkey = region.r_regionkey),
                              alist(filter(((nation.n_name = k0.n_name) &&
                                           (k2 = orders.o_orderdate)),
                                      join((supplier.s_nationkey =
                                           nation.n_nationkey),
                                        join(((lineitem.l_suppkey =
                                              supplier.s_suppkey) &&
                                             (customer.c_nationkey =
                                             supplier.s_nationkey)),
                                          join((lineitem.l_orderkey =
                                               orders.o_orderkey),
                                            join((customer.c_custkey =
                                                 orders.o_custkey),
                                              customer,
                                              orders),
                                            lineitem),
                                          supplier),
                                        nation)),
                                atuple([ascalar(lineitem.l_extendedprice),
                                        ascalar(lineitem.l_discount),
                                        ascalar(nation.n_name),
                                        ascalar(nation.n_regionkey)],
                                  cross)))],
                      cross),
                    param0))),
              (param1 + day(1)),
              ((param1 + year(1)) + day(1)))))) |}]

  let%expect_test "" =
    let r =
      of_string_exn
        {|
        select([(sum((partsupp.ps_supplycost * partsupp.ps_availqty)) * param2) as v],
      ahashidx(dedup(
                 select([nation.n_name as k1],
                   atuple([alist(partsupp,
                             atuple([ascalar(partsupp.ps_partkey),
                                     ascalar(partsupp.ps_suppkey),
                                     ascalar(partsupp.ps_availqty),
                                     ascalar(partsupp.ps_supplycost),
                                     ascalar(partsupp.ps_comment)],
                               cross)),
                           ahashidx(dedup(
                                      select([partsupp.ps_suppkey as k0],
                                        alist(partsupp,
                                          atuple([ascalar(partsupp.ps_partkey),
                                                  ascalar(partsupp.ps_suppkey),
                                                  ascalar(partsupp.ps_availqty),
                                                  ascalar(partsupp.ps_supplycost),
                                                  ascalar(partsupp.ps_comment)],
                                            cross)))),
                             alist(filter((supplier.s_suppkey = k0),
                                     join((supplier.s_nationkey =
                                          nation.n_nationkey),
                                       nation,
                                       supplier)),
                               atuple([ascalar(nation.n_nationkey),
                                       ascalar(nation.n_name),
                                       ascalar(nation.n_regionkey),
                                       ascalar(nation.n_comment),
                                       ascalar(supplier.s_suppkey),
                                       ascalar(supplier.s_name),
                                       ascalar(supplier.s_address),
                                       ascalar(supplier.s_nationkey),
                                       ascalar(supplier.s_phone),
                                       ascalar(supplier.s_acctbal),
                                       ascalar(supplier.s_comment)],
                                 cross)),
                             partsupp.ps_suppkey)],
                     cross))),
        atuple([alist(partsupp,
                  atuple([ascalar(partsupp.ps_partkey),
                          ascalar(partsupp.ps_suppkey),
                          ascalar(partsupp.ps_availqty),
                          ascalar(partsupp.ps_supplycost),
                          ascalar(partsupp.ps_comment)],
                    cross)),
                ahashidx(dedup(
                           select([partsupp.ps_suppkey as k0],
                             alist(partsupp,
                               atuple([ascalar(partsupp.ps_partkey),
                                       ascalar(partsupp.ps_suppkey),
                                       ascalar(partsupp.ps_availqty),
                                       ascalar(partsupp.ps_supplycost),
                                       ascalar(partsupp.ps_comment)],
                                 cross)))),
                  alist(filter((k1 = nation.n_name),
                          filter((supplier.s_suppkey = k0),
                            join((supplier.s_nationkey = nation.n_nationkey),
                              nation,
                              supplier))),
                    atuple([ascalar(nation.n_nationkey),
                            ascalar(nation.n_name),
                            ascalar(nation.n_regionkey),
                            ascalar(nation.n_comment),
                            ascalar(supplier.s_suppkey),
                            ascalar(supplier.s_name),
                            ascalar(supplier.s_address),
                            ascalar(supplier.s_nationkey),
                            ascalar(supplier.s_phone),
                            ascalar(supplier.s_acctbal),
                            ascalar(supplier.s_comment)],
                      cross)),
                  partsupp.ps_suppkey)],
          cross),
        param1))
|}
    in
    project
      ~params:
        (Set.of_list
           (module Name)
           [ Name.create ~type_:(StringT {nullable= false}) "param1"
           ; Name.create ~type_:(IntT {nullable= false}) "param2" ])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      select([(sum((partsupp.ps_supplycost * partsupp.ps_availqty)) * param2) as v],
        ahashidx(dedup(
                   select([nation.n_name as k1],
                     atuple([alist(partsupp, ascalar(partsupp.ps_suppkey)),
                             ahashidx(dedup(
                                        select([partsupp.ps_suppkey as k0],
                                          alist(partsupp,
                                            ascalar(partsupp.ps_suppkey)))),
                               alist(filter((supplier.s_suppkey = k0),
                                       join((supplier.s_nationkey =
                                            nation.n_nationkey),
                                         nation,
                                         supplier)),
                                 ascalar(nation.n_name)),
                               partsupp.ps_suppkey)],
                       cross))),
          atuple([alist(partsupp,
                    atuple([ascalar(partsupp.ps_suppkey),
                            ascalar(partsupp.ps_availqty),
                            ascalar(partsupp.ps_supplycost)],
                      cross)),
                  ahashidx(dedup(
                             select([partsupp.ps_suppkey as k0],
                               alist(partsupp, ascalar(partsupp.ps_suppkey)))),
                    alist(filter((k1 = nation.n_name),
                            filter((supplier.s_suppkey = k0),
                              join((supplier.s_nationkey = nation.n_nationkey),
                                nation,
                                supplier))),
                      ascalar(null)),
                    partsupp.ps_suppkey)],
            cross),
          param1)) |}]
end
