open Core
open Abslayout
open Name

module Config = struct
  module type S = sig
    val conn : Db.t
  end
end

module Make (C : Config.S) = struct
  open C

  (** Given a context containing names and a new name, determine which of the
     existing names corresponds and annotate the new name with the same type. *)
  let resolve_name ctx n =
    let could_not_resolve =
      Error.create "Could not resolve." (n, ctx) [%sexp_of: t * Set.M(Name).t]
    in
    if Option.is_some (rel n) then
      (* If the name has a relational part, then it must exactly match a name in the
         set. *)
      match Set.find ctx ~f:(fun n' -> O.(n' = n)) with
      | Some n' -> n'
      | None -> Error.raise could_not_resolve
    else
      (* If the name has no relational part, first try to resolve it to another
         name that also lacks a relational part. *)
      match
        Set.find ctx ~f:(fun n' ->
            Option.is_none (rel n') && String.(name n = name n') )
      with
      | Some n' -> n'
      | None -> (
          (* If no such name exists, then resolve it only if there is exactly one
             name where the field parts match. Otherwise the name is ambiguous. *)
          let matches =
            Set.to_list ctx |> List.filter ~f:(fun n' -> String.(name n = name n'))
          in
          match matches with
          | [] -> Error.raise could_not_resolve
          | [n'] -> n'
          | n' :: n'' :: _ ->
              Error.create "Ambiguous name." (n, n', n'') [%sexp_of: t * t * t]
              |> Error.raise )

  let set_stage s ctx =
    Set.map (module Name) ctx ~f:(fun n -> Name.Meta.(set n stage s))

  let resolve_relation stage r_name =
    let r = Db.Relation.from_db conn r_name in
    List.map r.fields ~f:(fun f -> create ~relation:r.rname ~type_:f.type_ f.fname)
    |> Set.of_list (module Name)
    |> set_stage stage

  let rename name s = Set.map (module Name) s ~f:(copy ~relation:(Some name))

  let empty_ctx = Set.empty (module Name)

  let preds_to_names preds =
    List.map preds ~f:Pred.to_schema
    |> List.filter ~f:(fun n -> String.(name n <> ""))
    |> Set.of_list (module Name)

  let pred_to_name pred =
    let n = Pred.to_schema pred in
    if String.(name n = "") then None else Some n

  let union c1 c2 = Set.union c1 c2

  let union_list = List.fold_left ~init:(Set.empty (module Name)) ~f:union

  let rec resolve_pred ctx =
    let visitor =
      object
        inherit [_] endo

        method! visit_Name ctx _ n = Name (resolve_name ctx n)

        method! visit_Exists ctx _ r =
          let r', _ = resolve `Run ctx r in
          Exists r'

        method! visit_First ctx _ r =
          let r', _ = resolve `Run ctx r in
          First r'
      end
    in
    visitor#visit_pred ctx

  and resolve stage outer_ctx {node; meta} =
    let rsame = resolve stage in
    let node', ctx' =
      match node with
      | Select (preds, r) ->
          let r, preds =
            let r, inner_ctx = rsame outer_ctx r in
            let ctx = union outer_ctx inner_ctx in
            (r, List.map preds ~f:(resolve_pred ctx))
          in
          (Select (preds, r), preds_to_names preds)
      | Filter (pred, r) ->
          let r, value_ctx = rsame outer_ctx r in
          let pred = resolve_pred (union outer_ctx value_ctx) pred in
          (Filter (pred, r), value_ctx)
      | Join {pred; r1; r2} ->
          let r1, inner_ctx1 = rsame outer_ctx r1 in
          let r2, inner_ctx2 = rsame outer_ctx r2 in
          let ctx = union_list [inner_ctx1; inner_ctx2; outer_ctx] in
          let pred = resolve_pred ctx pred in
          (Join {pred; r1; r2}, union inner_ctx1 inner_ctx2)
      | Scan l -> (Scan l, resolve_relation stage l)
      | GroupBy (aggs, key, r) ->
          let r, inner_ctx = rsame outer_ctx r in
          let ctx = union outer_ctx inner_ctx in
          let aggs = List.map ~f:(resolve_pred ctx) aggs in
          let key = List.map key ~f:(resolve_name ctx) in
          (GroupBy (aggs, key, r), preds_to_names aggs)
      | Dedup r ->
          let r, inner_ctx = rsame outer_ctx r in
          (Dedup r, inner_ctx)
      | AEmpty -> (AEmpty, empty_ctx)
      | AScalar p ->
          let p = resolve_pred outer_ctx p in
          let ctx =
            match pred_to_name p with
            | Some n -> Set.singleton (module Name) n
            | None -> empty_ctx
          in
          (AScalar p, set_stage `Run ctx)
      | AList (r, l) ->
          let r, outer_ctx' = resolve `Compile outer_ctx r in
          let l, ctx = rsame (union outer_ctx' outer_ctx) l in
          (AList (r, l), ctx)
      | ATuple (ls, (Zip as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          let ctx = union_list ctxs in
          (ATuple (ls, t), ctx)
      | ATuple (ls, (Concat as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          let ctx = List.hd_exn ctxs in
          (ATuple (ls, t), ctx)
      | ATuple (ls, (Cross as t)) ->
          let ls, ctx =
            List.fold_left ls ~init:([], empty_ctx) ~f:(fun (ls, ctx) l ->
                let l, ctx' = rsame (union outer_ctx ctx) l in
                (l :: ls, union ctx ctx') )
          in
          (ATuple (List.rev ls, t), ctx)
      | AHashIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          let l, value_ctx = rsame (union outer_ctx key_ctx) l in
          let m =
            (object
               inherit [_] map

               method! visit_pred _ = resolve_pred outer_ctx
            end)
              #visit_hash_idx () m
          in
          (AHashIdx (r, l, m), Set.union key_ctx value_ctx)
      | AOrderedIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          let l, value_ctx = rsame (union outer_ctx key_ctx) l in
          let m =
            (object
               inherit [_] map

               method! visit_pred _ = resolve_pred outer_ctx
            end)
              #visit_ordered_idx () m
          in
          (AOrderedIdx (r, l, m), Set.union key_ctx value_ctx)
      | As (n, r) ->
          let r, ctx = rsame outer_ctx r in
          let ctx = rename n ctx in
          (As (n, r), ctx)
      | OrderBy {key; rel} ->
          let rel, inner_ctx = rsame outer_ctx rel in
          let key = List.map key ~f:(fun (p, o) -> (resolve_pred inner_ctx p, o)) in
          (OrderBy {key; rel}, inner_ctx)
    in
    let ctx' = set_stage stage ctx' in
    ({node= node'; meta}, ctx')

  (** Annotate names in an algebra expression with types. *)
  let resolve ?(params = Set.empty (module Name)) r =
    let params = set_stage `Run params in
    let r, _ = resolve `Run params r in
    r
end

module Test = struct
  module C = struct
    let conn = Db.create "postgresql:///tpch_1k"
  end

  module T = Make (C)
  open T

  let pp, _ = mk_pp ~pp_name:Name.pp_with_stage ()

  let%expect_test "" =
    let r =
      {|
      alist(orderby([k0.l_shipmode],
         select([lineitem.l_shipmode],
           dedup(select([lineitem.l_shipmode], lineitem))) as k0),
   select([lineitem.l_shipmode,
           sum((if ((orders.o_orderpriority = "1-URGENT") ||
                   (orders.o_orderpriority = "2-HIGH")) then 1 else 0)) as high_line_count,
           sum((if (not((orders.o_orderpriority = "1-URGENT")) &&
                   not((orders.o_orderpriority = "2-HIGH"))) then 1 else 0)) as low_line_count],
     filter(((lineitem.l_shipmode = k0.l_shipmode) &&
            (((lineitem.l_shipmode = param1) ||
             (lineitem.l_shipmode = param2)) &&
            ((lineitem.l_commitdate < lineitem.l_receiptdate) &&
            ((lineitem.l_shipdate < lineitem.l_commitdate) &&
            ((lineitem.l_receiptdate >= param3) &&
            (lineitem.l_receiptdate < (param3 + year(1)))))))),
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
           cross)))))
|}
      |> of_string_exn
    in
    resolve
      ~params:
        (Set.of_list
           (module Name)
           [Name.create "param1"; Name.create "param2"; Name.create "param3"])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      alist(orderby([k0.l_shipmode@comp],
              select([lineitem.l_shipmode@comp],
                dedup(select([lineitem.l_shipmode@comp], lineitem))) as k0),
        select([lineitem.l_shipmode@run,
                sum((if ((orders.o_orderpriority@run = "1-URGENT") ||
                        (orders.o_orderpriority@run = "2-HIGH")) then 1 else 0)) as high_line_count,
                sum((if (not((orders.o_orderpriority@run = "1-URGENT")) &&
                        not((orders.o_orderpriority@run = "2-HIGH"))) then 1 else 0)) as low_line_count],
          filter(((lineitem.l_shipmode@run = k0.l_shipmode@comp) &&
                 (((lineitem.l_shipmode@run = param1@run) ||
                  (lineitem.l_shipmode@run = param2@run)) &&
                 ((lineitem.l_commitdate@run < lineitem.l_receiptdate@run) &&
                 ((lineitem.l_shipdate@run < lineitem.l_commitdate@run) &&
                 ((lineitem.l_receiptdate@run >= param3@run) &&
                 (lineitem.l_receiptdate@run < (param3@run + year(1)))))))),
            alist(join((orders.o_orderkey@comp = lineitem.l_orderkey@comp),
                    lineitem,
                    orders),
              atuple([ascalar(lineitem.l_orderkey@comp),
                      ascalar(lineitem.l_partkey@comp),
                      ascalar(lineitem.l_suppkey@comp),
                      ascalar(lineitem.l_linenumber@comp),
                      ascalar(lineitem.l_quantity@comp),
                      ascalar(lineitem.l_extendedprice@comp),
                      ascalar(lineitem.l_discount@comp),
                      ascalar(lineitem.l_tax@comp),
                      ascalar(lineitem.l_returnflag@comp),
                      ascalar(lineitem.l_linestatus@comp),
                      ascalar(lineitem.l_shipdate@comp),
                      ascalar(lineitem.l_commitdate@comp),
                      ascalar(lineitem.l_receiptdate@comp),
                      ascalar(lineitem.l_shipinstruct@comp),
                      ascalar(lineitem.l_shipmode@comp),
                      ascalar(lineitem.l_comment@comp),
                      ascalar(orders.o_orderkey@comp),
                      ascalar(orders.o_custkey@comp),
                      ascalar(orders.o_orderstatus@comp),
                      ascalar(orders.o_totalprice@comp),
                      ascalar(orders.o_orderdate@comp),
                      ascalar(orders.o_orderpriority@comp),
                      ascalar(orders.o_clerk@comp),
                      ascalar(orders.o_shippriority@comp),
                      ascalar(orders.o_comment@comp)],
                cross))))) |}]

  let%expect_test "" =
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
    resolve
      ~params:
        (Set.of_list (module Name) [Name.create "param0"; Name.create "param1"])
      r
    |> Format.printf "%a@." pp;
    [%expect {|
      select([nation.n_name@run, revenue@run],
        alist(select([nation.n_name@comp],
                dedup(select([nation.n_name@comp], nation))) as k0,
          select([nation.n_name@run, sum(agg3@run) as revenue],
            aorderedidx(dedup(
                          select([orders.o_orderdate@comp as k2],
                            dedup(select([orders.o_orderdate@comp], orders)))),
              filter((count4@run > 0),
                select([count() as count4,
                        sum((lineitem.l_extendedprice@run *
                            (1 - lineitem.l_discount@run))) as agg3,
                        k1@run,
                        region.r_regionkey@run,
                        region.r_name@run,
                        region.r_comment@run,
                        customer.c_custkey@run,
                        customer.c_name@run,
                        customer.c_address@run,
                        customer.c_nationkey@run,
                        customer.c_phone@run,
                        customer.c_acctbal@run,
                        customer.c_mktsegment@run,
                        customer.c_comment@run,
                        orders.o_orderkey@run,
                        orders.o_custkey@run,
                        orders.o_orderstatus@run,
                        orders.o_totalprice@run,
                        orders.o_orderdate@run,
                        orders.o_orderpriority@run,
                        orders.o_clerk@run,
                        orders.o_shippriority@run,
                        orders.o_comment@run,
                        lineitem.l_orderkey@run,
                        lineitem.l_partkey@run,
                        lineitem.l_suppkey@run,
                        lineitem.l_linenumber@run,
                        lineitem.l_quantity@run,
                        lineitem.l_extendedprice@run,
                        lineitem.l_discount@run,
                        lineitem.l_tax@run,
                        lineitem.l_returnflag@run,
                        lineitem.l_linestatus@run,
                        lineitem.l_shipdate@run,
                        lineitem.l_commitdate@run,
                        lineitem.l_receiptdate@run,
                        lineitem.l_shipinstruct@run,
                        lineitem.l_shipmode@run,
                        lineitem.l_comment@run,
                        supplier.s_suppkey@run,
                        supplier.s_name@run,
                        supplier.s_address@run,
                        supplier.s_nationkey@run,
                        supplier.s_phone@run,
                        supplier.s_acctbal@run,
                        supplier.s_comment@run,
                        nation.n_nationkey@run,
                        nation.n_name@run,
                        nation.n_regionkey@run,
                        nation.n_comment@run],
                  ahashidx(dedup(
                             select([region.r_name@comp as k1],
                               atuple([alist(region,
                                         atuple([ascalar(region.r_regionkey@comp),
                                                 ascalar(region.r_name@comp),
                                                 ascalar(region.r_comment@comp)],
                                           cross)),
                                       filter((nation.n_regionkey@comp =
                                              region.r_regionkey@comp),
                                         alist(join((supplier.s_nationkey@comp =
                                                    nation.n_nationkey@comp),
                                                 join(((lineitem.l_suppkey@comp =
                                                       supplier.s_suppkey@comp) &&
                                                      (customer.c_nationkey@comp =
                                                      supplier.s_nationkey@comp)),
                                                   join((lineitem.l_orderkey@comp =
                                                        orders.o_orderkey@comp),
                                                     join((customer.c_custkey@comp
                                                          = orders.o_custkey@comp),
                                                       customer,
                                                       orders),
                                                     lineitem),
                                                   supplier),
                                                 nation),
                                           atuple([ascalar(customer.c_custkey@comp),
                                                   ascalar(customer.c_name@comp),
                                                   ascalar(customer.c_address@comp),
                                                   ascalar(customer.c_nationkey@comp),
                                                   ascalar(customer.c_phone@comp),
                                                   ascalar(customer.c_acctbal@comp),
                                                   ascalar(customer.c_mktsegment@comp),
                                                   ascalar(customer.c_comment@comp),
                                                   ascalar(orders.o_orderkey@comp),
                                                   ascalar(orders.o_custkey@comp),
                                                   ascalar(orders.o_orderstatus@comp),
                                                   ascalar(orders.o_totalprice@comp),
                                                   ascalar(orders.o_orderdate@comp),
                                                   ascalar(orders.o_orderpriority@comp),
                                                   ascalar(orders.o_clerk@comp),
                                                   ascalar(orders.o_shippriority@comp),
                                                   ascalar(orders.o_comment@comp),
                                                   ascalar(lineitem.l_orderkey@comp),
                                                   ascalar(lineitem.l_partkey@comp),
                                                   ascalar(lineitem.l_suppkey@comp),
                                                   ascalar(lineitem.l_linenumber@comp),
                                                   ascalar(lineitem.l_quantity@comp),
                                                   ascalar(lineitem.l_extendedprice@comp),
                                                   ascalar(lineitem.l_discount@comp),
                                                   ascalar(lineitem.l_tax@comp),
                                                   ascalar(lineitem.l_returnflag@comp),
                                                   ascalar(lineitem.l_linestatus@comp),
                                                   ascalar(lineitem.l_shipdate@comp),
                                                   ascalar(lineitem.l_commitdate@comp),
                                                   ascalar(lineitem.l_receiptdate@comp),
                                                   ascalar(lineitem.l_shipinstruct@comp),
                                                   ascalar(lineitem.l_shipmode@comp),
                                                   ascalar(lineitem.l_comment@comp),
                                                   ascalar(supplier.s_suppkey@comp),
                                                   ascalar(supplier.s_name@comp),
                                                   ascalar(supplier.s_address@comp),
                                                   ascalar(supplier.s_nationkey@comp),
                                                   ascalar(supplier.s_phone@comp),
                                                   ascalar(supplier.s_acctbal@comp),
                                                   ascalar(supplier.s_comment@comp),
                                                   ascalar(nation.n_nationkey@comp),
                                                   ascalar(nation.n_name@comp),
                                                   ascalar(nation.n_regionkey@comp),
                                                   ascalar(nation.n_comment@comp)],
                                             cross)))],
                                 cross))),
                    atuple([alist(filter((k1@comp = region.r_name@comp), region),
                              atuple([ascalar(region.r_regionkey@comp),
                                      ascalar(region.r_name@comp),
                                      ascalar(region.r_comment@comp)],
                                cross)),
                            filter((nation.n_regionkey@run =
                                   region.r_regionkey@run),
                              alist(filter(((nation.n_name@comp = k0.n_name@comp)
                                           && (k2@comp = orders.o_orderdate@comp)),
                                      join((supplier.s_nationkey@comp =
                                           nation.n_nationkey@comp),
                                        join(((lineitem.l_suppkey@comp =
                                              supplier.s_suppkey@comp) &&
                                             (customer.c_nationkey@comp =
                                             supplier.s_nationkey@comp)),
                                          join((lineitem.l_orderkey@comp =
                                               orders.o_orderkey@comp),
                                            join((customer.c_custkey@comp =
                                                 orders.o_custkey@comp),
                                              customer,
                                              orders),
                                            lineitem),
                                          supplier),
                                        nation)),
                                atuple([ascalar(customer.c_custkey@comp),
                                        ascalar(customer.c_name@comp),
                                        ascalar(customer.c_address@comp),
                                        ascalar(customer.c_nationkey@comp),
                                        ascalar(customer.c_phone@comp),
                                        ascalar(customer.c_acctbal@comp),
                                        ascalar(customer.c_mktsegment@comp),
                                        ascalar(customer.c_comment@comp),
                                        ascalar(orders.o_orderkey@comp),
                                        ascalar(orders.o_custkey@comp),
                                        ascalar(orders.o_orderstatus@comp),
                                        ascalar(orders.o_totalprice@comp),
                                        ascalar(orders.o_orderdate@comp),
                                        ascalar(orders.o_orderpriority@comp),
                                        ascalar(orders.o_clerk@comp),
                                        ascalar(orders.o_shippriority@comp),
                                        ascalar(orders.o_comment@comp),
                                        ascalar(lineitem.l_orderkey@comp),
                                        ascalar(lineitem.l_partkey@comp),
                                        ascalar(lineitem.l_suppkey@comp),
                                        ascalar(lineitem.l_linenumber@comp),
                                        ascalar(lineitem.l_quantity@comp),
                                        ascalar(lineitem.l_extendedprice@comp),
                                        ascalar(lineitem.l_discount@comp),
                                        ascalar(lineitem.l_tax@comp),
                                        ascalar(lineitem.l_returnflag@comp),
                                        ascalar(lineitem.l_linestatus@comp),
                                        ascalar(lineitem.l_shipdate@comp),
                                        ascalar(lineitem.l_commitdate@comp),
                                        ascalar(lineitem.l_receiptdate@comp),
                                        ascalar(lineitem.l_shipinstruct@comp),
                                        ascalar(lineitem.l_shipmode@comp),
                                        ascalar(lineitem.l_comment@comp),
                                        ascalar(supplier.s_suppkey@comp),
                                        ascalar(supplier.s_name@comp),
                                        ascalar(supplier.s_address@comp),
                                        ascalar(supplier.s_nationkey@comp),
                                        ascalar(supplier.s_phone@comp),
                                        ascalar(supplier.s_acctbal@comp),
                                        ascalar(supplier.s_comment@comp),
                                        ascalar(nation.n_nationkey@comp),
                                        ascalar(nation.n_name@comp),
                                        ascalar(nation.n_regionkey@comp),
                                        ascalar(nation.n_comment@comp)],
                                  cross)))],
                      cross),
                    param0@run))),
              (param1@run + day(1)),
              ((param1@run + year(1)) + day(1)))))) |}]
end
