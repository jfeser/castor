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
    let pp, _ = mk_pp ~pp_name:Name.pp_with_stage () in
    resolve
      ~params:
        (Set.of_list
           (module Name)
           [Name.create "param1"; Name.create "param2"; Name.create "param3"])
      r
    |> Format.printf "%a@." pp;
    [%expect {|
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
end
