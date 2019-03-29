open Core
open Abslayout

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
  subquery_needed_visitor#visit_t () r ;
  needed (of_list Meta.(find_exn r schema)) r

let project r =
  let dummy = Set.empty (module Name) in
  let select_needed r =
    let schema = Meta.(find_exn r schema) in
    let needed = Meta.(find_exn r needed) in
    if List.for_all schema ~f:(Set.mem needed) then r
    else select (Set.to_list needed |> List.map ~f:(fun n -> Name n)) r
  in
  let project_visitor =
    object (self : 'a)
      inherit [_] map as super

      method! visit_Select needed (ps, r) =
        let ps' =
          List.filter ps ~f:(fun p ->
              match pred_to_name p with None -> false | Some n -> Set.mem needed n
          )
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
        AList (select_needed rk, self#visit_t dummy rv)

      method! visit_AHashIdx _ (rk, rv, idx) =
        AHashIdx (select_needed rk, self#visit_t dummy rv, idx)

      method! visit_AOrderedIdx _ (rk, rv, idx) =
        AOrderedIdx (select_needed rk, self#visit_t dummy rv, idx)

      method! visit_t _ r =
        let needed = Meta.(find_exn r needed) in
        super#visit_t needed r
    end
  in
  annotate_free r ;
  annotate_needed r ;
  project_visitor#visit_t dummy r

module Test = struct
  let%expect_test "project" =
    let module M = Abslayout_db.Make (struct
      let conn = Db.create "postgresql:///tpch_1k"
    end) in
    let r =
      Abslayout.of_string_exn
        {|
        select([lineitem.l_orderkey,
        revenue,
        orders.o_orderdate,
        orders.o_shippriority],
  alist(join(true,
          dedup(select([lineitem.l_orderkey], lineitem)),
          dedup(select([orders.o_shippriority, orders.o_orderdate], orders))) as k0,
    select([lineitem.l_orderkey,
            sum((lineitem.l_extendedprice * (1 - lineitem.l_discount))) as revenue,
            orders.o_orderdate,
            orders.o_shippriority],
      filter((lineitem.l_shipdate > param1),
        aorderedidx(dedup(
                      select([orders.o_orderdate as k5],
                        dedup(select([orders.o_orderdate], orders)))),
          alist(filter((k5 = orders.o_orderdate),
                  dedup(select([orders.o_orderdate], orders))),
            atuple([ascalar(orders.o_orderdate)], cross)),
          date("0000-01-01"),
          (param1 + day(1)))))))
|}
    in
    let r =
      M.resolve
        ~params:
          (Set.of_list
             (module Name)
             [Name.create ~type_:(DateT {nullable= false}) "param1"])
        r
    in
    M.annotate_schema r ;
    project r |> Format.printf "%a@." Abslayout.pp
end
