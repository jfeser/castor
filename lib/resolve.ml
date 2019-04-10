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

  let mut_refcnt =
    Univ_map.Key.create ~name:"mut-refcnt" [%sexp_of: int ref Map.M(Name).t]

  let refcnt = Univ_map.Key.create ~name:"refcnt" [%sexp_of: int Map.M(Name).t]

  let fix_meta_visitor =
    object
      inherit [_] map as super

      method! visit_Name () n =
        match Meta.find n meta_ref with
        | Some m -> Name (copy n ~meta:!m)
        | None -> Name n

      method! visit_t () ({meta= _; _} as r) =
        let ({meta; _} as r) = super#visit_t () r in
        (* Replace mutable refcounts with immutable refcounts. *)
        ( match Univ_map.find !meta mut_refcnt with
        | Some defs ->
            meta := Map.map defs ~f:( ! ) |> Univ_map.set !meta refcnt ;
            meta := Univ_map.remove !meta mut_refcnt
        | None -> () ) ;
        r
    end

  module Ctx = struct
    module T : sig
      type row =
        { rname: Name.t
        ; rstage: [`Run | `Compile]
        ; rmeta: Univ_map.t ref
        ; rrefs: int ref list }

      type t = private row list [@@deriving sexp_of]

      val of_list : row list -> t
    end = struct
      type row =
        { rname: Name.t
        ; rstage: [`Run | `Compile]
        ; rmeta: Univ_map.t ref
        ; rrefs: int ref list }
      [@@deriving sexp_of]

      type t = row list [@@deriving sexp_of]

      let of_list l =
        let l =
          List.map l ~f:(fun r ->
              {r with rname= Name.Meta.(set r.rname stage r.rstage)} )
        in
        let compare r1 r2 =
          [%compare: Name.t * [`Run | `Compile]] (r1.rname, r1.rstage)
            (r2.rname, r2.rstage)
        in
        List.find_all_dups l ~compare
        |> List.iter ~f:(fun r ->
               Logs.warn (fun m -> m "Shadowing of %a." Name.pp_with_stage r.rname)
           ) ;
        List.dedup_and_sort ~compare l
    end

    include T

    let merge (c1 : t) (c2 : t) : t = of_list ((c1 :> row list) @ (c2 :> row list))

    let merge_list (ls : t list) : t = List.concat (ls :> row list list) |> of_list

    let find_name (m : t) rel f =
      let m = (m :> row list) in
      match rel with
      | `Rel r ->
          let k = Name.create ~relation:r f in
          List.find m ~f:(fun r -> Name.O.(r.rname = k))
      | `NoRel ->
          let k = Name.create f in
          List.find m ~f:(fun r -> Name.O.(r.rname = k))
      | `AnyRel -> (
        match List.filter m ~f:(fun r -> String.(name r.rname = f)) with
        | [] -> None
        | [r] -> Some r
        | r' :: r'' :: _ ->
            Logs.err (fun m ->
                m "Ambiguous name %s: could refer to %a or %a" f Name.pp r'.rname
                  Name.pp r''.rname ) ;
            None )

    let in_stage (c : t) s =
      List.filter
        (c :> row list)
        ~f:(fun r -> [%compare.equal: [`Run | `Compile]] r.rstage s)

    let find s (m : t) rel f =
      match
        ( find_name (in_stage m `Run |> of_list) rel f
        , find_name (in_stage m `Compile |> of_list) rel f )
      with
      | Some v, None | None, Some v -> Some v
      | None, None -> None
      | Some mr, Some mc -> (
          let to_name rel f =
            match rel with
            | `Rel r -> Name.create ~relation:r f
            | _ -> Name.create f
          in
          Logs.warn (fun m ->
              m "Cross-stage shadowing of %a." Name.pp (to_name rel f) ) ;
          match s with `Run -> Some mr | `Compile -> Some mc )

    let incr_refs s (m : t) =
      let incr_ref {rrefs; _} = List.iter rrefs ~f:incr in
      in_stage m s |> List.iter ~f:incr_ref

    let to_schema p =
      let t =
        (* NOTE: Must first put type metadata back into names. *)
        Pred.to_type (fix_meta_visitor#visit_pred () p)
      in
      Option.map (Pred.to_name p) ~f:(Name.copy ~type_:(Some t))

    let of_defs s ps : _ * t =
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
                let meta = ref meta in
                let p = (visitor n meta)#visit_pred () p in
                (Some (n, meta), p)
            | None -> (None, p) )
        |> List.unzip
      in
      let ctx =
        List.filter_map metas
          ~f:
            (Option.map ~f:(fun (n, meta) ->
                 {rname= n; rmeta= meta; rrefs= [ref 0]; rstage= s} ))
      in
      (defs, of_list ctx)

    let add_refcnts (ctx : t) : t * _ =
      let ctx, refcounts =
        List.map
          (ctx :> row list)
          ~f:(fun r ->
            let rc = ref 0 in
            ({r with rrefs= rc :: r.rrefs}, (r.rname, rc)) )
        |> List.unzip
      in
      let refcounts =
        Map.of_alist_multi (module Name) refcounts
        |> Map.mapi ~f:(fun ~key:n ~data ->
               match data with
               | [x] -> x
               | x :: _ ->
                   Logs.warn (fun m -> m "Output shadowing of %a." Name.pp n) ;
                   x
               | _ -> assert false )
      in
      (of_list ctx, refcounts)

    let rename (ctx : t) name : t =
      List.map
        ~f:(fun r -> {r with rname= copy ~relation:(Some name) r.rname})
        (ctx :> row list)
      |> of_list
  end

  (** Given a context containing names and a new name, determine which of the
     existing names corresponds and annotate the new name with the same type. *)
  let resolve_name s ctx n =
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
    List.iter m.rrefs ~f:incr ; m.rname

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

  and resolve stage outer_ctx ({node; meta} as r) =
    let rsame = resolve stage in
    let resolve' = function
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
      | AEmpty -> (AEmpty, Ctx.of_list [])
      | AScalar p ->
          let p = resolve_pred `Compile outer_ctx p in
          let def, ctx =
            match Ctx.of_defs stage [p] with
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
            List.fold_left ls
              ~init:([], Ctx.of_list [])
              ~f:(fun (ls, ctx) l ->
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
    let node, ctx =
      try resolve' node
      with exn ->
        let pp () x =
          Abslayout.pp Format.str_formatter x ;
          Format.flush_str_formatter ()
        in
        Exn.reraisef exn "Resolving: %a" pp r ()
    in
    let ctx, refcnts = Ctx.add_refcnts ctx in
    meta := Univ_map.set !meta mut_refcnt refcnts ;
    ({node; meta}, ctx)

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

  let pp, _ = mk_pp ~pp_name:pp_with_stage ()

  let pp_with_refcount, _ =
    mk_pp ~pp_name:pp_with_stage
      ~pp_meta:(fun fmt meta ->
        let open Format in
        match Univ_map.find meta refcnt with
        | Some r ->
            fprintf fmt "@[<hv 2>{" ;
            Map.iteri r ~f:(fun ~key:n ~data:c ->
                if c > 0 then fprintf fmt "%a=%d,@ " Name.pp n c ) ;
            fprintf fmt "}@]"
        | None -> () )
      ()

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
    resolve r |> Format.printf "%a@." pp_with_refcount ;
    [%expect
      {|
      select([lineitem.l_receiptdate@run],
        alist(join((orders.o_orderkey@comp = lineitem.l_orderkey@comp),
                lineitem#{lineitem.l_commitdate=1,
                           lineitem.l_orderkey=2,
                           lineitem.l_receiptdate=1,
                           },
                orders#{orders.o_comment=1, orders.o_orderkey=1, })#{lineitem.l_commitdate=1,
                                                                      lineitem.l_orderkey=1,
                                                                      lineitem.l_receiptdate=1,
                                                                      orders.o_comment=1,
                                                                      },
          atuple([ascalar(lineitem.l_orderkey@comp)#{},
                  ascalar(lineitem.l_commitdate@comp)#{},
                  ascalar(lineitem.l_receiptdate@comp)#{lineitem.l_receiptdate=1, },
                  ascalar(orders.o_comment@comp)#{}],
            cross)#{lineitem.l_receiptdate=1, })#{lineitem.l_receiptdate=1, })#
        {lineitem.l_receiptdate=1, } |}]

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
        (Set.of_list
           (module Name)
           [ create ~type_:(StringT {nullable= false}) "param0"
           ; create ~type_:(DateT {nullable= false}) "param1" ])
      r
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

  let%expect_test "" =
    let r =
      of_string_exn
        {|
  alist(select([n1_name,
                                                           n2_name,
                                                           l_year,
                                                           revenue],
                                                     orderby([n1_name,
                                                              n2_name,
                                                              l_year],
                                                       groupby([n1_name,
                                                                n2_name,
                                                                l_year,
                                                                sum(volume) as revenue],
                                                         [n1_name,
                                                          n2_name,
                                                          l_year],
                                                         select([n1_name,
                                                                 n2_name,
                                                                 to_year(lineitem.l_shipdate) as l_year,
                                                                 (lineitem.l_extendedprice
                                                                 *
                                                                 (1 -
                                                                 lineitem.l_discount)) as volume],
                                                           atuple([alist(
                                                                    select(
                                                                    [nation.n_name as n1_name,
                                                                    nation.n_nationkey as n1_nationkey],
                                                                    nation),
                                                                    atuple(
                                                                    [ascalar(n1_name),
                                                                    ascalar(n1_nationkey)],
                                                                    cross)),
                                                                   filter(
                                                                    (supplier.s_nationkey
                                                                    =
                                                                    n1_nationkey),
                                                                    alist(
                                                                    join(
                                                                    (customer.c_nationkey
                                                                    =
                                                                    n2_nationkey),
                                                                    join(
                                                                    (supplier.s_suppkey
                                                                    =
                                                                    lineitem.l_suppkey),
                                                                    join(
                                                                    (customer.c_custkey
                                                                    =
                                                                    orders.o_custkey),
                                                                    join(
                                                                    (orders.o_orderkey
                                                                    =
                                                                    lineitem.l_orderkey),
                                                                    filter(
                                                                    (
                                                                    (lineitem.l_shipdate
                                                                    >=
                                                                    date("1995-01-01"))
                                                                    &&
                                                                    (lineitem.l_shipdate
                                                                    <=
                                                                    date("1996-12-31"))),
                                                                    lineitem),
                                                                    orders),
                                                                    customer),
                                                                    supplier),
                                                                    select(
                                                                    [nation.n_name as n2_name,
                                                                    nation.n_nationkey as n2_nationkey],
                                                                    nation)),
                                                                    atuple(
                                                                    [ascalar(lineitem.l_orderkey),
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
                                                                    ascalar(orders.o_comment),
                                                                    ascalar(customer.c_custkey),
                                                                    ascalar(customer.c_name),
                                                                    ascalar(customer.c_address),
                                                                    ascalar(customer.c_nationkey),
                                                                    ascalar(customer.c_phone),
                                                                    ascalar(customer.c_acctbal),
                                                                    ascalar(customer.c_mktsegment),
                                                                    ascalar(customer.c_comment),
                                                                    ascalar(supplier.s_suppkey),
                                                                    ascalar(supplier.s_name),
                                                                    ascalar(supplier.s_address),
                                                                    ascalar(supplier.s_nationkey),
                                                                    ascalar(supplier.s_phone),
                                                                    ascalar(supplier.s_acctbal),
                                                                    ascalar(supplier.s_comment),
                                                                    ascalar(n2_name),
                                                                    ascalar(n2_nationkey)],
                                                                    cross)))],
                                                             cross))))),
                                               atuple([ascalar(n1_name),
                                                       ascalar(n2_name),
                                                       ascalar(l_year),
                                                       ascalar(revenue)],
                                                 cross))
|}
    in
    resolve r |> Format.printf "%a@." pp;
    [%expect {|
      alist(select([n1_name@comp, n2_name@comp, l_year@comp, revenue@comp],
              orderby([n1_name@comp, n2_name@comp, l_year@comp],
                groupby([n1_name@comp,
                         n2_name@comp,
                         l_year@comp,
                         sum(volume@comp) as revenue],
                  [n1_name@comp, n2_name@comp, l_year@comp],
                  select([n1_name@comp,
                          n2_name@comp,
                          to_year(lineitem.l_shipdate@comp) as l_year,
                          (lineitem.l_extendedprice@comp *
                          (1 - lineitem.l_discount@comp)) as volume],
                    atuple([alist(select([nation.n_name@comp as n1_name,
                                          nation.n_nationkey@comp as n1_nationkey],
                                    nation),
                              atuple([ascalar(n1_name@comp),
                                      ascalar(n1_nationkey@comp)],
                                cross)),
                            filter((supplier.s_nationkey@comp = n1_nationkey@comp),
                              alist(join((customer.c_nationkey@comp =
                                         n2_nationkey@comp),
                                      join((supplier.s_suppkey@comp =
                                           lineitem.l_suppkey@comp),
                                        join((customer.c_custkey@comp =
                                             orders.o_custkey@comp),
                                          join((orders.o_orderkey@comp =
                                               lineitem.l_orderkey@comp),
                                            filter(((lineitem.l_shipdate@comp >=
                                                    date("1995-01-01")) &&
                                                   (lineitem.l_shipdate@comp <=
                                                   date("1996-12-31"))),
                                              lineitem),
                                            orders),
                                          customer),
                                        supplier),
                                      select([nation.n_name@comp as n2_name,
                                              nation.n_nationkey@comp as n2_nationkey],
                                        nation)),
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
                                        ascalar(orders.o_comment@comp),
                                        ascalar(customer.c_custkey@comp),
                                        ascalar(customer.c_name@comp),
                                        ascalar(customer.c_address@comp),
                                        ascalar(customer.c_nationkey@comp),
                                        ascalar(customer.c_phone@comp),
                                        ascalar(customer.c_acctbal@comp),
                                        ascalar(customer.c_mktsegment@comp),
                                        ascalar(customer.c_comment@comp),
                                        ascalar(supplier.s_suppkey@comp),
                                        ascalar(supplier.s_name@comp),
                                        ascalar(supplier.s_address@comp),
                                        ascalar(supplier.s_nationkey@comp),
                                        ascalar(supplier.s_phone@comp),
                                        ascalar(supplier.s_acctbal@comp),
                                        ascalar(supplier.s_comment@comp),
                                        ascalar(n2_name@comp),
                                        ascalar(n2_nationkey@comp)],
                                  cross)))],
                      cross))))),
        atuple([ascalar(n1_name@comp),
                ascalar(n2_name@comp),
                ascalar(l_year@comp),
                ascalar(revenue@comp)],
          cross)) |}]
end
