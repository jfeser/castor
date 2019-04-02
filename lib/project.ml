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

  (** Annotate all subexpressions with the set of needed fields. A field is needed
   if it is in the schema of the top level query or it is in the free variable
   set of a query in scope. *)
  let annotate_needed r =
    let singleton = Set.singleton (module Name) in
    let of_list = Set.of_list (module Name) in
    let union_list = Set.union_list (module Name) in
    let rec needed ctx r =
      Meta.(set_m r needed ctx) ;
      match r.node with
      | Scan _ | AScalar _ | AEmpty -> ()
      | Select (ps, r') -> needed (List.map ps ~f:pred_free |> union_list) r'
      | Filter (p, r') -> needed (Set.union ctx (pred_free p)) r'
      | Dedup r' -> needed ctx r'
      | Join {pred; r1; r2} ->
          let ctx' = Set.union (pred_free pred) ctx in
          needed ctx' r1 ; needed ctx' r2
      | GroupBy (ps, key, r') ->
          let ctx' =
            List.map ps ~f:pred_free |> union_list |> Set.union (of_list key)
          in
          needed ctx' r'
      | OrderBy {key; rel; _} ->
          let ctx' =
            Set.union ctx (List.map ~f:(fun (p, _) -> pred_free p) key |> union_list)
          in
          needed ctx' rel
      | AList (pr, cr) | AHashIdx (pr, cr, _) | AOrderedIdx (pr, cr, _) ->
          needed Meta.(find_exn cr free) pr ;
          needed ctx cr
      | ATuple (rs, Zip) ->
          List.iter rs ~f:(fun r ->
              needed (Set.inter ctx (Meta.(find_exn r schema) |> of_list)) r )
      | ATuple (rs, Concat) -> List.iter rs ~f:(needed ctx)
      | ATuple (rs, Cross) ->
          List.fold_right rs ~init:ctx ~f:(fun r ctx ->
              needed ctx r ;
              let ctx = Set.union Meta.(find_exn r free) ctx in
              ctx )
          |> ignore
      | As (rel_name, r') ->
          let ctx' =
            List.filter
              Meta.(find_exn r' schema)
              ~f:(fun n -> Set.mem ctx (Name.copy n ~relation:(Some rel_name)))
            |> of_list
          in
          needed ctx' r'
    in
    let subquery_needed_visitor =
      object
        inherit [_] iter

        method! visit_First () r =
          match Meta.(find_exn r schema) with
          | n :: _ -> needed (singleton n) r
          | [] -> failwith "Unexpected empty schema."

        method! visit_Exists () r =
          (* TODO: None of these fields are really needed. Use the first one
             because it's simple. *)
          match Meta.(find_exn r schema) with
          | n :: _ -> needed (singleton n) r
          | [] -> failwith "Unexpected empty schema."
      end
    in
    annotate_free r ;
    M.annotate_schema r ;
    subquery_needed_visitor#visit_t () r ;
    needed (of_list Meta.(find_exn r schema)) r

  (** Select the fields in `r` that are needed. *)
  let select_needed r =
    let schema = Meta.(find_exn r schema) in
    let needed = Meta.(find_exn r needed) in
    let select_list =
      List.filter schema ~f:(Set.mem needed) |> List.map ~f:Pred.name
    in
    (* Don't emit selects of the entire schema. *)
    if List.length select_list < List.length schema then select select_list r else r

  (* let project r =
   *   let dummy = Set.empty (module Name) in
   *   let project_visitor =
   *     object (self : 'a)
   *       inherit [_] map as super
   * 
   *       method! visit_Select needed (ps, r) =
   *         let ps' =
   *           List.filter ps ~f:(fun p ->
   *               match pred_to_name p with
   *               | None -> false
   *               | Some n -> Set.mem needed n )
   *         in
   *         Select (ps', self#visit_t dummy r)
   * 
   *       method! visit_ATuple needed (rs, k) =
   *         let rs' =
   *           List.filter rs ~f:(fun r ->
   *               let s = Meta.(find_exn r schema) |> Set.of_list (module Name) in
   *               let is_unneeded = Set.is_empty (Set.inter s needed) in
   *               if is_unneeded then
   *                 Logs.debug ~src:test (fun m ->
   *                     m "Removing tuple element (needed=%a): %a" (Set.pp Name.pp)
   *                       needed Abslayout.pp r ) ;
   *               not is_unneeded )
   *           |> List.map ~f:(self#visit_t dummy)
   *         in
   *         ATuple (rs', k)
   * 
   *       method! visit_AList _ (rk, rv) =
   *         AList (self#visit_t dummy rk, self#visit_t dummy rv)
   * 
   *       method! visit_AHashIdx _ (rk, rv, idx) =
   *         AHashIdx (self#visit_t dummy rk, self#visit_t dummy rv, idx)
   * 
   *       method! visit_AOrderedIdx _ (rk, rv, idx) =
   *         AOrderedIdx (self#visit_t dummy rk, self#visit_t dummy rv, idx)
   * 
   *       method! visit_t _ r =
   *         let needed = Meta.(find_exn r needed) in
   *         super#visit_t needed r
   *     end
   *   in
   *   let r = strip_meta r in
   *   M.annotate_schema r ;
   *   annotate_needed r ;
   *   project_visitor#visit_t dummy r *)

  let project_defs ps =
    List.filter ps ~f:(fun p ->
        match pred_to_name p with
        | None ->
            (* Filter out definitions that have no name *)
            false
        | Some n -> (
          (* Filter out definitions that are never referenced. *)
          match Name.Meta.(find n refcnt) with
          | Some c -> c > 0
          | None ->
              (* Be conservative if refcount is missing. *)
              true ) )

  let project r =
    let project_visitor =
      object (self : 'a)
        inherit [_] endo as super

        method! visit_Select () _ (ps, r) =
          Select (project_defs ps, self#visit_t () r)

        method! visit_GroupBy () _ ps ns r =
          GroupBy (project_defs ps, ns, self#visit_t () r)

        method! visit_AScalar () _ p =
          match project_defs [p] with
          | [] -> AScalar Null
          | [p] -> AScalar p
          | _ -> assert false

        method! visit_ATuple () t (ps, k) =
          match super#visit_ATuple () t (ps, k) with
          | ATuple (ps, (Cross as k)) ->
              let ps =
                List.filter ps ~f:(fun r ->
                    match r.node with AScalar Null -> false | _ -> true )
              in
              (* Preserve a single scalar to avoid messing up count aggregates.
                 *)
              let ps = if List.is_empty ps then [scalar Null] else ps in
              ATuple (ps, k)
          | ATuple _ as a -> a
          | _ -> assert false
      end
    in
    project_visitor#visit_t () r
end

module Test = struct
  module T = Make (struct
    let conn = Db.create "postgresql:///tpch_1k"
  end)

  open T

  let pp, _ = mk_pp ~pp_name:Name.pp_with_stage_and_refcnt ()

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
                           atuple([ascalar(lineitem.l_orderkey),
                                   ascalar(lineitem.l_partkey),
                                   ascalar(lineitem.l_suppkey),
                                   ascalar(lineitem.l_linenumber),
                                   ascalar(lineitem.l_quantity),
                                   ascalar(lineitem.l_extendedprice),
                                   ascalar(lineitem.l_discount),
                                   ascalar(lineitem.l_tax),
                                   ascalar(lineitem.l_returnflag),
                                   ascalar(lineitem.l_linestatus),
                                   ascalar(lineitem.l_shipdate),
                                   ascalar(lineitem.l_commitdate),
                                   ascalar(lineitem.l_receiptdate),
                                   ascalar(lineitem.l_shipinstruct),
                                   ascalar(lineitem.l_shipmode),
                                   ascalar(lineitem.l_comment),
                                   ascalar(orders.o_orderkey),
                                   ascalar(orders.o_custkey),
                                   ascalar(orders.o_orderstatus),
                                   ascalar(orders.o_totalprice),
                                   ascalar(orders.o_orderdate),
                                   ascalar(orders.o_orderpriority),
                                   ascalar(orders.o_clerk),
                                   ascalar(orders.o_shippriority),
                                   ascalar(orders.o_comment)],
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
    M.resolve
      ~params:
        (Set.of_list
           (module Name)
           [ Name.create ~type_:(StringT {nullable= false}) "param1"
           ; Name.create ~type_:(StringT {nullable= false}) "param2"
           ; Name.create ~type_:(DateT {nullable= false}) "param3" ])
      r
    |> project |> Format.printf "%a@." pp ;
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
                              alist(join((orders.o_orderkey = lineitem.l_orderkey),
                                      lineitem,
                                      orders),
                                atuple([ascalar(lineitem.l_receiptdate)], cross)))))),
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
                   k1,
                   region.r_regionkey,
                   region.r_name,
                   region.r_comment,
                   customer.c_custkey,
                   customer.c_name,
                   customer.c_address,
                   customer.c_nationkey,
                   customer.c_phone,
                   customer.c_acctbal,
                   customer.c_mktsegment,
                   customer.c_comment,
                   orders.o_orderkey,
                   orders.o_custkey,
                   orders.o_orderstatus,
                   orders.o_totalprice,
                   orders.o_orderdate,
                   orders.o_orderpriority,
                   orders.o_clerk,
                   orders.o_shippriority,
                   orders.o_comment,
                   lineitem.l_orderkey,
                   lineitem.l_partkey,
                   lineitem.l_suppkey,
                   lineitem.l_linenumber,
                   lineitem.l_quantity,
                   lineitem.l_extendedprice,
                   lineitem.l_discount,
                   lineitem.l_tax,
                   lineitem.l_returnflag,
                   lineitem.l_linestatus,
                   lineitem.l_shipdate,
                   lineitem.l_commitdate,
                   lineitem.l_receiptdate,
                   lineitem.l_shipinstruct,
                   lineitem.l_shipmode,
                   lineitem.l_comment,
                   supplier.s_suppkey,
                   supplier.s_name,
                   supplier.s_address,
                   supplier.s_nationkey,
                   supplier.s_phone,
                   supplier.s_acctbal,
                   supplier.s_comment,
                   nation.n_nationkey,
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
                                      atuple([ascalar(customer.c_custkey),
                                              ascalar(customer.c_name),
                                              ascalar(customer.c_address),
                                              ascalar(customer.c_nationkey),
                                              ascalar(customer.c_phone),
                                              ascalar(customer.c_acctbal),
                                              ascalar(customer.c_mktsegment),
                                              ascalar(customer.c_comment),
                                              ascalar(orders.o_orderkey),
                                              ascalar(orders.o_custkey),
                                              ascalar(orders.o_orderstatus),
                                              ascalar(orders.o_totalprice),
                                              ascalar(orders.o_orderdate),
                                              ascalar(orders.o_orderpriority),
                                              ascalar(orders.o_clerk),
                                              ascalar(orders.o_shippriority),
                                              ascalar(orders.o_comment),
                                              ascalar(lineitem.l_orderkey),
                                              ascalar(lineitem.l_partkey),
                                              ascalar(lineitem.l_suppkey),
                                              ascalar(lineitem.l_linenumber),
                                              ascalar(lineitem.l_quantity),
                                              ascalar(lineitem.l_extendedprice),
                                              ascalar(lineitem.l_discount),
                                              ascalar(lineitem.l_tax),
                                              ascalar(lineitem.l_returnflag),
                                              ascalar(lineitem.l_linestatus),
                                              ascalar(lineitem.l_shipdate),
                                              ascalar(lineitem.l_commitdate),
                                              ascalar(lineitem.l_receiptdate),
                                              ascalar(lineitem.l_shipinstruct),
                                              ascalar(lineitem.l_shipmode),
                                              ascalar(lineitem.l_comment),
                                              ascalar(supplier.s_suppkey),
                                              ascalar(supplier.s_name),
                                              ascalar(supplier.s_address),
                                              ascalar(supplier.s_nationkey),
                                              ascalar(supplier.s_phone),
                                              ascalar(supplier.s_acctbal),
                                              ascalar(supplier.s_comment),
                                              ascalar(nation.n_nationkey),
                                              ascalar(nation.n_name),
                                              ascalar(nation.n_regionkey),
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
                           atuple([ascalar(customer.c_custkey),
                                   ascalar(customer.c_name),
                                   ascalar(customer.c_address),
                                   ascalar(customer.c_nationkey),
                                   ascalar(customer.c_phone),
                                   ascalar(customer.c_acctbal),
                                   ascalar(customer.c_mktsegment),
                                   ascalar(customer.c_comment),
                                   ascalar(orders.o_orderkey),
                                   ascalar(orders.o_custkey),
                                   ascalar(orders.o_orderstatus),
                                   ascalar(orders.o_totalprice),
                                   ascalar(orders.o_orderdate),
                                   ascalar(orders.o_orderpriority),
                                   ascalar(orders.o_clerk),
                                   ascalar(orders.o_shippriority),
                                   ascalar(orders.o_comment),
                                   ascalar(lineitem.l_orderkey),
                                   ascalar(lineitem.l_partkey),
                                   ascalar(lineitem.l_suppkey),
                                   ascalar(lineitem.l_linenumber),
                                   ascalar(lineitem.l_quantity),
                                   ascalar(lineitem.l_extendedprice),
                                   ascalar(lineitem.l_discount),
                                   ascalar(lineitem.l_tax),
                                   ascalar(lineitem.l_returnflag),
                                   ascalar(lineitem.l_linestatus),
                                   ascalar(lineitem.l_shipdate),
                                   ascalar(lineitem.l_commitdate),
                                   ascalar(lineitem.l_receiptdate),
                                   ascalar(lineitem.l_shipinstruct),
                                   ascalar(lineitem.l_shipmode),
                                   ascalar(lineitem.l_comment),
                                   ascalar(supplier.s_suppkey),
                                   ascalar(supplier.s_name),
                                   ascalar(supplier.s_address),
                                   ascalar(supplier.s_nationkey),
                                   ascalar(supplier.s_phone),
                                   ascalar(supplier.s_acctbal),
                                   ascalar(supplier.s_comment),
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
    let rec fix f x =
      let x' = f x in
      if Abslayout.O.(x = x') then x' else fix f x'
    in
    Logs.Src.set_level test (Some Logs.Debug) ;
    Logs.(set_reporter (format_reporter ())) ;
    fix
      (fun r ->
        let r' =
          M.resolve
            ~params:
              (Set.of_list
                 (module Name)
                 [ Name.create ~type_:(StringT {nullable= false}) "param0"
                 ; Name.create ~type_:(DateT {nullable= false}) "param1" ])
            r
          |> project
        in
        Format.printf "%a@.============\n" pp r' ;
        r' )
      r
    |> ignore
end
