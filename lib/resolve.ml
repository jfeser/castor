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

      method! visit_Name () p n =
        match Meta.find n meta_ref with
        | Some m -> Name (copy n ~meta:!m)
        | None -> p
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

    let incr_refs s {r; c} =
      match s with
      | `Run -> Map.iter r ~f:incr_ref
      | `Compile -> Map.iter c ~f:incr_ref

    let to_schema p =
      let t =
        (* NOTE: Must first put type metadata back into names. *)
        Pred.to_type (fix_meta_visitor#visit_pred () p)
      in
      Option.map (Pred.to_name p) ~f:(Name.copy ~type_:(Some t))

    let of_defs s ps =
      let visitor def meta =
        object
          inherit [_] map

          method! visit_Name () n =
            if Name.O.(n = def) then Name Name.Meta.(set n meta_ref meta)
            else Name n
        end
      in
      (* Create a list of definitions with fresh metadata refs. *)
      let metas, defs =
        List.map ps ~f:(fun p ->
            match to_schema p with
            | Some n ->
                (* If this is a definition point, annotate it with fresh metadata
                 and expose it in the context. *)
                let meta =
                  match Meta.find n meta_ref with
                  | Some m -> !m
                  | None -> Name.meta n
                in
                let meta = Univ_map.set meta Meta.refcnt 0 in
                let meta = ref meta in
                let p = (visitor n meta)#visit_pred () p in
                (Some (n, meta), p)
            | None -> (None, p) )
        |> List.unzip
      in
      let ctx =
        let m =
          List.filter_map metas ~f:(function x -> x)
          |> Map.of_alist_exn (module Name)
        in
        let e = Map.empty (module Name) in
        match s with `Run -> create m e | `Compile -> create e m
      in
      (defs, ctx)

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
    (* TODO: Nowhere to put the defs here *)
    let _, ctx =
      List.map r.fields ~f:(fun f ->
          Name (create ~relation:r.rname ~type_:f.type_ f.fname) )
      |> Ctx.of_defs stage
    in
    ctx

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
          let defs, ctx = Ctx.of_defs stage preds in
          (Select (defs, r), ctx)
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
          let defs, ctx = Ctx.of_defs stage aggs in
          (GroupBy (defs, key, r), ctx)
      | Dedup r ->
          let r, inner_ctx = rsame outer_ctx r in
          (Dedup r, inner_ctx)
      | AEmpty -> (AEmpty, Ctx.empty)
      | AScalar p ->
          let p = resolve_pred `Compile outer_ctx p in
          let def, ctx =
            match Ctx.of_defs `Run [p] with
            | [def], ctx -> (def, ctx)
            | _ -> assert false
          in
          (AScalar def, ctx)
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
    let _, ctx =
      Ctx.of_defs `Run (Set.to_list params |> List.map ~f:(fun n -> Name n))
    in
    let r, ctx = resolve `Run ctx r in
    (* Ensure that all the outputs are referenced. *)
    Ctx.incr_refs `Run ctx ;
    fix_meta_visitor#visit_t () r
end

module Test = struct
  module C = struct
    let conn = Db.create "postgresql:///tpch_1k"
  end

  module T = Make (C)
  open T

  let pp, _ = mk_pp ~pp_name:pp_with_stage_and_refcnt ()

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
           [ create ~type_:(StringT {nullable= false}) "param1"
           ; create ~type_:(StringT {nullable= false}) "param2"
           ; create ~type_:(DateT {nullable= false}) "param3" ])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      alist(orderby([k0.l_shipmode@comp#2],
              select([lineitem.l_shipmode@comp#2],
                dedup(select([lineitem.l_shipmode@comp#1], lineitem))) as k0),
        select([lineitem.l_shipmode@run#1,
                sum((if ((orders.o_orderpriority@run#4 = "1-URGENT") ||
                        (orders.o_orderpriority@run#4 = "2-HIGH")) then 1 else 0)) as high_line_count,
                sum((if (not((orders.o_orderpriority@run#4 = "1-URGENT")) &&
                        not((orders.o_orderpriority@run#4 = "2-HIGH"))) then 1 else 0)) as low_line_count],
          filter(((lineitem.l_shipmode@run#4 = k0.l_shipmode@comp#2) &&
                 (((lineitem.l_shipmode@run#4 = param1@run#1) ||
                  (lineitem.l_shipmode@run#4 = param2@run#1)) &&
                 ((lineitem.l_commitdate@run#2 < lineitem.l_receiptdate@run#3) &&
                 ((lineitem.l_shipdate@run#1 < lineitem.l_commitdate@run#2) &&
                 ((lineitem.l_receiptdate@run#3 >= param3@run#2) &&
                 (lineitem.l_receiptdate@run#3 < (param3@run#2 + year(1)))))))),
            alist(join((orders.o_orderkey@comp#2 = lineitem.l_orderkey@comp#2),
                    lineitem,
                    orders),
              atuple([ascalar(lineitem.l_orderkey@run#0),
                      ascalar(lineitem.l_partkey@run#0),
                      ascalar(lineitem.l_suppkey@run#0),
                      ascalar(lineitem.l_linenumber@run#0),
                      ascalar(lineitem.l_quantity@run#0),
                      ascalar(lineitem.l_extendedprice@run#0),
                      ascalar(lineitem.l_discount@run#0),
                      ascalar(lineitem.l_tax@run#0),
                      ascalar(lineitem.l_returnflag@run#0),
                      ascalar(lineitem.l_linestatus@run#0),
                      ascalar(lineitem.l_shipdate@run#1),
                      ascalar(lineitem.l_commitdate@run#2),
                      ascalar(lineitem.l_receiptdate@run#3),
                      ascalar(lineitem.l_shipinstruct@run#0),
                      ascalar(lineitem.l_shipmode@run#4),
                      ascalar(lineitem.l_comment@run#0),
                      ascalar(orders.o_orderkey@run#0),
                      ascalar(orders.o_custkey@run#0),
                      ascalar(orders.o_orderstatus@run#0),
                      ascalar(orders.o_totalprice@run#0),
                      ascalar(orders.o_orderdate@run#0),
                      ascalar(orders.o_orderpriority@run#4),
                      ascalar(orders.o_clerk@run#0),
                      ascalar(orders.o_shippriority@run#0),
                      ascalar(orders.o_comment@run#0)],
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
        (Set.of_list
           (module Name)
           [ create ~type_:(StringT {nullable= false}) "param0"
           ; create ~type_:(DateT {nullable= false}) "param1" ])
      r
    |> Format.printf "%a@." pp ;
    [%expect
      {|
      select([nation.n_name@run#1, revenue@run#1],
        alist(select([nation.n_name@comp#1],
                dedup(select([nation.n_name@comp#1], nation))) as k0,
          select([nation.n_name@run#1, sum(agg3@run#1) as revenue],
            aorderedidx(dedup(
                          select([orders.o_orderdate@comp#1 as k2],
                            dedup(select([orders.o_orderdate@comp#1], orders)))),
              filter((count4@run#1 > 0),
                select([count() as count4,
                        sum((lineitem.l_extendedprice@run#2 *
                            (1 - lineitem.l_discount@run#2))) as agg3,
                        k1@run#0,
                        region.r_regionkey@run#0,
                        region.r_name@run#0,
                        region.r_comment@run#0,
                        customer.c_custkey@run#0,
                        customer.c_name@run#0,
                        customer.c_address@run#0,
                        customer.c_nationkey@run#0,
                        customer.c_phone@run#0,
                        customer.c_acctbal@run#0,
                        customer.c_mktsegment@run#0,
                        customer.c_comment@run#0,
                        orders.o_orderkey@run#0,
                        orders.o_custkey@run#0,
                        orders.o_orderstatus@run#0,
                        orders.o_totalprice@run#0,
                        orders.o_orderdate@run#0,
                        orders.o_orderpriority@run#0,
                        orders.o_clerk@run#0,
                        orders.o_shippriority@run#0,
                        orders.o_comment@run#0,
                        lineitem.l_orderkey@run#0,
                        lineitem.l_partkey@run#0,
                        lineitem.l_suppkey@run#0,
                        lineitem.l_linenumber@run#0,
                        lineitem.l_quantity@run#0,
                        lineitem.l_extendedprice@run#0,
                        lineitem.l_discount@run#0,
                        lineitem.l_tax@run#0,
                        lineitem.l_returnflag@run#0,
                        lineitem.l_linestatus@run#0,
                        lineitem.l_shipdate@run#0,
                        lineitem.l_commitdate@run#0,
                        lineitem.l_receiptdate@run#0,
                        lineitem.l_shipinstruct@run#0,
                        lineitem.l_shipmode@run#0,
                        lineitem.l_comment@run#0,
                        supplier.s_suppkey@run#0,
                        supplier.s_name@run#0,
                        supplier.s_address@run#0,
                        supplier.s_nationkey@run#0,
                        supplier.s_phone@run#0,
                        supplier.s_acctbal@run#0,
                        supplier.s_comment@run#0,
                        nation.n_nationkey@run#0,
                        nation.n_name@run#1,
                        nation.n_regionkey@run#0,
                        nation.n_comment@run#0],
                  ahashidx(dedup(
                             select([region.r_name@run#1 as k1],
                               atuple([alist(region,
                                         atuple([ascalar(region.r_regionkey@run#1),
                                                 ascalar(region.r_name@run#1),
                                                 ascalar(region.r_comment@run#0)],
                                           cross)),
                                       filter((nation.n_regionkey@run#1 =
                                              region.r_regionkey@run#1),
                                         alist(join((supplier.s_nationkey@comp#3 =
                                                    nation.n_nationkey@comp#2),
                                                 join(((lineitem.l_suppkey@comp#2 =
                                                       supplier.s_suppkey@comp#2)
                                                      &&
                                                      (customer.c_nationkey@comp#2
                                                      =
                                                      supplier.s_nationkey@comp#3)),
                                                   join((lineitem.l_orderkey@comp#2
                                                        = orders.o_orderkey@comp#2),
                                                     join((customer.c_custkey@comp#2
                                                          =
                                                          orders.o_custkey@comp#2),
                                                       customer,
                                                       orders),
                                                     lineitem),
                                                   supplier),
                                                 nation),
                                           atuple([ascalar(customer.c_custkey@run#0),
                                                   ascalar(customer.c_name@run#0),
                                                   ascalar(customer.c_address@run#0),
                                                   ascalar(customer.c_nationkey@run#0),
                                                   ascalar(customer.c_phone@run#0),
                                                   ascalar(customer.c_acctbal@run#0),
                                                   ascalar(customer.c_mktsegment@run#0),
                                                   ascalar(customer.c_comment@run#0),
                                                   ascalar(orders.o_orderkey@run#0),
                                                   ascalar(orders.o_custkey@run#0),
                                                   ascalar(orders.o_orderstatus@run#0),
                                                   ascalar(orders.o_totalprice@run#0),
                                                   ascalar(orders.o_orderdate@run#0),
                                                   ascalar(orders.o_orderpriority@run#0),
                                                   ascalar(orders.o_clerk@run#0),
                                                   ascalar(orders.o_shippriority@run#0),
                                                   ascalar(orders.o_comment@run#0),
                                                   ascalar(lineitem.l_orderkey@run#0),
                                                   ascalar(lineitem.l_partkey@run#0),
                                                   ascalar(lineitem.l_suppkey@run#0),
                                                   ascalar(lineitem.l_linenumber@run#0),
                                                   ascalar(lineitem.l_quantity@run#0),
                                                   ascalar(lineitem.l_extendedprice@run#0),
                                                   ascalar(lineitem.l_discount@run#0),
                                                   ascalar(lineitem.l_tax@run#0),
                                                   ascalar(lineitem.l_returnflag@run#0),
                                                   ascalar(lineitem.l_linestatus@run#0),
                                                   ascalar(lineitem.l_shipdate@run#0),
                                                   ascalar(lineitem.l_commitdate@run#0),
                                                   ascalar(lineitem.l_receiptdate@run#0),
                                                   ascalar(lineitem.l_shipinstruct@run#0),
                                                   ascalar(lineitem.l_shipmode@run#0),
                                                   ascalar(lineitem.l_comment@run#0),
                                                   ascalar(supplier.s_suppkey@run#0),
                                                   ascalar(supplier.s_name@run#0),
                                                   ascalar(supplier.s_address@run#0),
                                                   ascalar(supplier.s_nationkey@run#0),
                                                   ascalar(supplier.s_phone@run#0),
                                                   ascalar(supplier.s_acctbal@run#0),
                                                   ascalar(supplier.s_comment@run#0),
                                                   ascalar(nation.n_nationkey@run#0),
                                                   ascalar(nation.n_name@run#0),
                                                   ascalar(nation.n_regionkey@run#1),
                                                   ascalar(nation.n_comment@run#0)],
                                             cross)))],
                                 cross))),
                    atuple([alist(filter((k1@comp#2 = region.r_name@comp#2),
                                    region),
                              atuple([ascalar(region.r_regionkey@run#2),
                                      ascalar(region.r_name@run#1),
                                      ascalar(region.r_comment@run#1)],
                                cross)),
                            filter((nation.n_regionkey@run#2 =
                                   region.r_regionkey@run#2),
                              alist(filter(((nation.n_name@comp#2 =
                                            k0.n_name@comp#1) &&
                                           (k2@comp#1 = orders.o_orderdate@comp#2)),
                                      join((supplier.s_nationkey@comp#3 =
                                           nation.n_nationkey@comp#2),
                                        join(((lineitem.l_suppkey@comp#2 =
                                              supplier.s_suppkey@comp#2) &&
                                             (customer.c_nationkey@comp#2 =
                                             supplier.s_nationkey@comp#3)),
                                          join((lineitem.l_orderkey@comp#2 =
                                               orders.o_orderkey@comp#2),
                                            join((customer.c_custkey@comp#2 =
                                                 orders.o_custkey@comp#2),
                                              customer,
                                              orders),
                                            lineitem),
                                          supplier),
                                        nation)),
                                atuple([ascalar(customer.c_custkey@run#1),
                                        ascalar(customer.c_name@run#1),
                                        ascalar(customer.c_address@run#1),
                                        ascalar(customer.c_nationkey@run#1),
                                        ascalar(customer.c_phone@run#1),
                                        ascalar(customer.c_acctbal@run#1),
                                        ascalar(customer.c_mktsegment@run#1),
                                        ascalar(customer.c_comment@run#1),
                                        ascalar(orders.o_orderkey@run#1),
                                        ascalar(orders.o_custkey@run#1),
                                        ascalar(orders.o_orderstatus@run#1),
                                        ascalar(orders.o_totalprice@run#1),
                                        ascalar(orders.o_orderdate@run#1),
                                        ascalar(orders.o_orderpriority@run#1),
                                        ascalar(orders.o_clerk@run#1),
                                        ascalar(orders.o_shippriority@run#1),
                                        ascalar(orders.o_comment@run#1),
                                        ascalar(lineitem.l_orderkey@run#1),
                                        ascalar(lineitem.l_partkey@run#1),
                                        ascalar(lineitem.l_suppkey@run#1),
                                        ascalar(lineitem.l_linenumber@run#1),
                                        ascalar(lineitem.l_quantity@run#1),
                                        ascalar(lineitem.l_extendedprice@run#2),
                                        ascalar(lineitem.l_discount@run#2),
                                        ascalar(lineitem.l_tax@run#1),
                                        ascalar(lineitem.l_returnflag@run#1),
                                        ascalar(lineitem.l_linestatus@run#1),
                                        ascalar(lineitem.l_shipdate@run#1),
                                        ascalar(lineitem.l_commitdate@run#1),
                                        ascalar(lineitem.l_receiptdate@run#1),
                                        ascalar(lineitem.l_shipinstruct@run#1),
                                        ascalar(lineitem.l_shipmode@run#1),
                                        ascalar(lineitem.l_comment@run#1),
                                        ascalar(supplier.s_suppkey@run#1),
                                        ascalar(supplier.s_name@run#1),
                                        ascalar(supplier.s_address@run#1),
                                        ascalar(supplier.s_nationkey@run#1),
                                        ascalar(supplier.s_phone@run#1),
                                        ascalar(supplier.s_acctbal@run#1),
                                        ascalar(supplier.s_comment@run#1),
                                        ascalar(nation.n_nationkey@run#1),
                                        ascalar(nation.n_name@run#1),
                                        ascalar(nation.n_regionkey@run#2),
                                        ascalar(nation.n_comment@run#1)],
                                  cross)))],
                      cross),
                    param0@run#1))),
              (param1@run#2 + day(1)),
              ((param1@run#2 + year(1)) + day(1)))))) |}]
end
