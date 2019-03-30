open Core
open Abslayout

module Config = struct
  module type S = sig
    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module M = Abslayout_db.Make (C)

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

  let project r =
    let dummy = Set.empty (module Name) in
    let project_visitor =
      object (self : 'a)
        inherit [_] map as super

        method! visit_Select needed (ps, r) =
          let ps' =
            List.filter ps ~f:(fun p ->
                match pred_to_name p with
                | None -> false
                | Some n -> Set.mem needed n )
          in
          Select (ps', self#visit_t dummy r)

        method! visit_ATuple needed (rs, k) =
          let rs' =
            List.filter rs ~f:(fun r ->
                let s = Meta.(find_exn r schema) |> Set.of_list (module Name) in
                not (Set.is_empty (Set.inter s needed)) )
            |> List.map ~f:(self#visit_t dummy)
          in
          ATuple (rs', k)

        method! visit_AList _ (rk, rv) =
          AList (self#visit_t dummy rk, self#visit_t dummy rv)

        method! visit_AHashIdx _ (rk, rv, idx) =
          AHashIdx (self#visit_t dummy rk, self#visit_t dummy rv, idx)

        method! visit_AOrderedIdx _ (rk, rv, idx) =
          AOrderedIdx (self#visit_t dummy rk, self#visit_t dummy rv, idx)

        method! visit_t _ r =
          let needed = Meta.(find_exn r needed) in
          super#visit_t needed r
      end
    in
    let r = strip_meta r in
    M.annotate_schema r ;
    annotate_needed r ;
    project_visitor#visit_t dummy r
end

module Test = struct
  module T = Make (struct
    let conn = Db.create "postgresql:///tpch_1k"
  end)

  open T

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
    project r |> Format.printf "%a@." pp ;
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
end
