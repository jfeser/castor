open Core
open Abslayout
open Collections
open Name

module Config = struct
  module type S = sig
    val conn : Db.t
  end
end

module Make (C : Config.S) = struct
  open C

  let meta_ref = Univ_map.Key.create ~name:"meta-ref" [%sexp_of: Univ_map.t ref]

  let fix_meta_visitor =
    object
      inherit [_] endo

      method! visit_Name () _ n = Name (copy n ~meta:!(Meta.find_exn n meta_ref))
    end

  module Ctx = struct
    module T : sig
      type m = Univ_map.t ref Map.M(Name).t

      type t = {c: m; r: m}

      val create : m -> m -> t
    end = struct
      type m = Univ_map.t ref Map.M(Name).t

      type t = {c: m; r: m}

      let create r c =
        Map.iter r ~f:(fun m -> m := Univ_map.set !m Meta.stage `Run) ;
        Map.iter c ~f:(fun m -> m := Univ_map.set !m Meta.stage `Compile) ;
        {r; c}
    end

    include T

    let sexp_of_t {r; c} =
      [%sexp_of: Name.t list * Name.t list] (Map.keys r, Map.keys c)

    let empty = create (Map.empty (module Name)) (Map.empty (module Name))

    let merge_single =
      Map.merge ~f:(fun ~key:n -> function
        | `Left x | `Right x -> Some x
        | `Both (_, _) -> Error.(create "Shadowing." n [%sexp_of: Name.t] |> raise)
      )

    let merge {r= r1; c= c1} {r= r2; c= c2} =
      create (merge_single r1 r2) (merge_single c1 c2)

    let merge_list = List.fold_left1 ~f:merge

    let find_name m rel f =
      match rel with
      | `Rel r -> Map.find m (Name.create ~relation:r f)
      | `NoRel -> Map.find m (Name.create f)
      | `AnyRel -> (
        match
          Map.to_alist m |> List.filter ~f:(fun (n', _) -> String.(name n' = f))
        with
        | [] -> None
        | [(_, m)] -> Some m
        | (n', _) :: (n'', _) :: _ ->
            Logs.err (fun m ->
                m "Ambiguous name %s: could refer to %a or %a" f Name.pp n' Name.pp
                  n'' ) ;
            None )

    let to_name rel f =
      match rel with `Rel r -> Name.create ~relation:r f | _ -> Name.create f

    let find s {r; c} rel f =
      match (find_name r rel f, find_name c rel f) with
      | Some v, None | None, Some v -> Some v
      | None, None -> None
      | Some mr, Some mc -> (
          Logs.warn (fun m ->
              m "Cross-stage shadowing of %a." Name.pp (to_name rel f) ) ;
          match s with `Run -> Some mr | `Compile -> Some mc )

    let incr_ref m =
      m :=
        Univ_map.change !m Meta.refcnt ~f:(function
          | Some x -> Some (x + 1)
          | None -> Some 1 )

    (** Create a context from a list of definitions. *)
    let of_names s ns =
      let m =
        List.map ns ~f:(fun n ->
            let meta =
              match Meta.find n meta_ref with Some m -> !m | None -> Name.meta n
            in
            let meta = Univ_map.set meta Meta.refcnt 0 in
            (n, ref meta) )
        |> Map.of_alist_exn (module Name)
      in
      let e = Map.empty (module Name) in
      match s with `Run -> create m e | `Compile -> create e m

    let to_schema p =
      let t =
        (* NOTE: Must first put type metadata back into names. *)
        Pred.to_type (fix_meta_visitor#visit_pred () p)
      in
      Option.map (Pred.to_name p) ~f:(Name.copy ~type_:(Some t))

    let of_preds s ps = List.filter_map ps ~f:to_schema |> of_names s

    let rename {r; c} name =
      let rn m =
        Map.to_alist m
        |> List.map ~f:(fun (n, m) -> (copy ~relation:(Some name) n, m))
        |> Map.of_alist_exn (module Name)
      in
      create (rn r) (rn c)
  end

  (** Given a context containing names and a new name, determine which of the
     existing names corresponds and annotate the new name with the same type. *)
  let resolve_name s (ctx : Ctx.t) n =
    let could_not_resolve =
      Error.create "Could not resolve." (n, ctx) [%sexp_of: t * Ctx.t]
    in
    let m =
      match rel n with
      | Some r -> (
        (* If the name has a relational part, then it must exactly match a
             name in the set. *)
        match Ctx.find s ctx (`Rel r) (name n) with
        | Some m -> m
        | None -> Error.raise could_not_resolve )
      | None -> (
        (* If the name has no relational part, first try to resolve it to
             another name that also lacks a relational part. *)
        match Ctx.find s ctx `NoRel (name n) with
        | Some m -> m
        | None -> (
          (* If no such name exists, then resolve it only if there is exactly
               one name where the field parts match. Otherwise the name is
               ambiguous. *)
          match Ctx.find s ctx `AnyRel (name n) with
          | Some m -> m
          | None -> Error.raise could_not_resolve ) )
    in
    Ctx.incr_ref m ;
    Meta.(set n meta_ref m)

  let resolve_relation stage r_name =
    let r = Db.Relation.from_db conn r_name in
    List.map r.fields ~f:(fun f -> create ~relation:r.rname ~type_:f.type_ f.fname)
    |> Ctx.of_names stage

  let rec resolve_pred stage (ctx : Ctx.t) =
    let visitor =
      object
        inherit [_] endo

        method! visit_Name ctx _ n = Name (resolve_name stage ctx n)

        method! visit_Exists ctx _ r =
          let r', _ = resolve `Run ctx r in
          Exists r'

        method! visit_First ctx _ r =
          let r', _ = resolve `Run ctx r in
          First r'
      end
    in
    visitor#visit_pred ctx

  and resolve stage (outer_ctx : Ctx.t) {node; meta} =
    let rsame = resolve stage in
    let node', ctx' =
      match node with
      | Select (preds, r) ->
          let r, preds =
            let r, inner_ctx = rsame outer_ctx r in
            let ctx = Ctx.merge outer_ctx inner_ctx in
            (r, List.map preds ~f:(resolve_pred stage ctx))
          in
          (Select (preds, r), Ctx.of_preds stage preds)
      | Filter (pred, r) ->
          let r, value_ctx = rsame outer_ctx r in
          let pred = resolve_pred stage (Ctx.merge outer_ctx value_ctx) pred in
          (Filter (pred, r), value_ctx)
      | Join {pred; r1; r2} ->
          let r1, inner_ctx1 = rsame outer_ctx r1 in
          let r2, inner_ctx2 = rsame outer_ctx r2 in
          let ctx = Ctx.merge_list [inner_ctx1; inner_ctx2; outer_ctx] in
          let pred = resolve_pred stage ctx pred in
          (Join {pred; r1; r2}, Ctx.merge inner_ctx1 inner_ctx2)
      | Scan l -> (Scan l, resolve_relation stage l)
      | GroupBy (aggs, key, r) ->
          let r, inner_ctx = rsame outer_ctx r in
          let ctx = Ctx.merge outer_ctx inner_ctx in
          let aggs = List.map ~f:(resolve_pred stage ctx) aggs in
          let key = List.map key ~f:(resolve_name stage ctx) in
          (GroupBy (aggs, key, r), Ctx.of_preds stage aggs)
      | Dedup r ->
          let r, inner_ctx = rsame outer_ctx r in
          (Dedup r, inner_ctx)
      | AEmpty -> (AEmpty, Ctx.empty)
      | AScalar p ->
          let p = resolve_pred `Compile outer_ctx p in
          let ctx = Ctx.of_preds `Run [p] in
          (AScalar p, ctx)
      | AList (r, l) ->
          let r, outer_ctx' = resolve `Compile outer_ctx r in
          let l, ctx = rsame (Ctx.merge outer_ctx' outer_ctx) l in
          (AList (r, l), ctx)
      | ATuple (ls, (Zip as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          (ATuple (ls, t), Ctx.merge_list ctxs)
      | ATuple (ls, (Concat as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          (ATuple (ls, t), List.hd_exn ctxs)
      | ATuple (ls, (Cross as t)) ->
          let ls, ctx =
            List.fold_left ls ~init:([], Ctx.empty) ~f:(fun (ls, ctx) l ->
                let l, ctx' = rsame (Ctx.merge outer_ctx ctx) l in
                (l :: ls, Ctx.merge ctx ctx') )
          in
          (ATuple (List.rev ls, t), ctx)
      | AHashIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          let l, value_ctx = rsame (Ctx.merge outer_ctx key_ctx) l in
          let m =
            (object
               inherit [_] map

               method! visit_pred _ = resolve_pred stage outer_ctx
            end)
              #visit_hash_idx () m
          in
          (AHashIdx (r, l, m), Ctx.merge key_ctx value_ctx)
      | AOrderedIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          let l, value_ctx = rsame (Ctx.merge outer_ctx key_ctx) l in
          let m =
            (object
               inherit [_] map

               method! visit_pred _ = resolve_pred stage outer_ctx
            end)
              #visit_ordered_idx () m
          in
          (AOrderedIdx (r, l, m), Ctx.merge key_ctx value_ctx)
      | As (n, r) ->
          let r, ctx = rsame outer_ctx r in
          (As (n, r), Ctx.rename ctx n)
      | OrderBy {key; rel} ->
          let rel, inner_ctx = rsame outer_ctx rel in
          let key =
            List.map key ~f:(fun (p, o) -> (resolve_pred stage inner_ctx p, o))
          in
          (OrderBy {key; rel}, inner_ctx)
    in
    ({node= node'; meta}, ctx')

  (** Annotate names in an algebra expression with types. *)
  let resolve ?(params = Set.empty (module Name)) r =
    let ctx = Ctx.of_names `Run (Set.to_list params) in
    let r, _ = resolve `Run ctx r in
    fix_meta_visitor#visit_t () r
end

module Test = struct
  module C = struct
    let conn = Db.create "postgresql:///tpch_1k"
  end

  module T = Make (C)
  open T

  let pp, _ = mk_pp ~pp_name:pp_with_stage ()

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
           [create "param1"; create "param2"; create "param3"])
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
    resolve ~params:(Set.of_list (module Name) [create "param0"; create "param1"]) r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
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
                        k1@comp,
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
                             select([region.r_name@run as k1],
                               atuple([alist(region,
                                         atuple([ascalar(region.r_regionkey@comp),
                                                 ascalar(region.r_name@comp),
                                                 ascalar(region.r_comment@comp)],
                                           cross)),
                                       filter((nation.n_regionkey@run =
                                              region.r_regionkey@run),
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
