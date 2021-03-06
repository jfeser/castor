open Printf
open Collections
open Ast
module V = Visitors
open Schema
module A = Abslayout
module P = Pred.Infix

module Config = struct
  module type S = sig
    val conn : Db.t

    val check_transforms : bool

    val params : Set.M(Name).t
  end
end

let rec normal_meta_pred p = V.Map.pred strip_meta normal_meta_pred p

module Make (Config : Config.S) () = struct
  type t = { name : string; f : Ast.t -> Ast.t list } [@@deriving sexp]

  let fresh = Global.fresh

  let db_relation n = A.relation (Db.relation Config.conn n)

  let run_everywhere ?(stage = `Both) tf =
    let f r =
      Path.all r
      |> Seq.concat_map ~f:(fun p ->
             let right_stage =
               match stage with
               | `Both -> true
               | (`Compile | `Run) as s -> Poly.(Path.stage_exn p r = s)
             in
             if right_stage then
               tf.f (Path.get_exn p r)
               |> List.map ~f:(Path.set_exn p r)
               |> Seq.of_list
             else Seq.empty)
      |> Seq.to_list
    in
    { tf with f }

  let run_unchecked t r =
    printf "Running %s\n" t.name;
    let rs =
      t.f r
      |> List.dedup_and_sort ~compare:[%compare: Ast.t]
      |> List.filter ~f:(fun r' -> not ([%compare.equal: Ast.t] r r'))
    in
    let len = List.length rs in
    if len > 0 then (
      List.iteri rs ~f:(fun i -> Format.printf "%d\n%a@.\n\n" i A.pp);
      Logs.info (fun m -> m "%d new candidates from running %s." len t.name) );
    rs

  let run_checked t r =
    let rs = run_unchecked t r in
    let check_schema r' =
      let s = schema r |> Set.of_list (module Name) in
      let s' = schema r' |> Set.of_list (module Name) in
      let schemas_ok = Set.is_subset s ~of_:s' in
      if not schemas_ok then
        Logs.warn (fun m ->
            m "Transform %s not equivalent. Schemas differ: %a %a" t.name
              Sexp.pp_hum
              ([%sexp_of: Set.M(Name).t] s)
              Sexp.pp_hum
              ([%sexp_of: Set.M(Name).t] s'));
      schemas_ok
    in
    let checks = [ check_schema ] in
    List.map rs ~f:(fun r' ->
        List.for_all checks ~f:(fun c -> c r') |> ignore;
        r')

  let run = if Config.check_transforms then run_checked else run_unchecked

  let no_params r = Set.is_empty (Set.inter (A.names r) Config.params)

  let replace_rel rel new_rel r =
    let visitor =
      object
        inherit [_] V.endo

        method! visit_Relation () r' { r_name = rel'; _ } =
          if String.(rel = rel') then new_rel.node else r'
      end
    in
    visitor#visit_t () r

  let tf_row_store _ =
    let open A in
    {
      name = "row-store";
      f =
        (fun r ->
          if no_params r then
            let scope = Fresh.name Global.fresh "s%d" in
            let scalars =
              Schema.scoped scope (schema r)
              |> List.map ~f:(fun n -> scalar (Name n))
            in
            [ list r scope (tuple scalars Cross) ]
          else []);
    }
    |> run_everywhere ~stage:`Run

  let tf_elim_groupby _ =
    let open A in
    {
      name = "elim-groupby";
      f =
        (function
        | { node = GroupBy (ps, key, r); _ } as rr when no_params rr ->
            let key_name = Fresh.name fresh "k%d" in
            let key_preds = List.map key ~f:P.name in
            let filter_pred =
              List.map key ~f:(fun n ->
                  Binop (Eq, Name n, Name (Name.copy n ~scope:(Some key_name))))
              |> List.fold_left ~init:(Bool true) ~f:(fun acc p ->
                     Binop (And, acc, p))
            in
            [
              list
                (dedup (select key_preds r))
                key_name
                (select ps (filter filter_pred r));
            ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_elim_groupby_partial _ =
    let open A in
    {
      name = "elim-groupby-partial";
      f =
        (function
        | { node = GroupBy (ps, key, r); _ } ->
            List.map key ~f:(fun k ->
                let key_name = Fresh.name fresh "k%d" in
                let scope = Fresh.name fresh "s%d" in
                let rel =
                  (Option.value_exn
                     (Db.relation_has_field Config.conn (Name.name k)))
                    .r_name
                in
                let lhs =
                  dedup
                    (select [ As_pred (Name k, key_name) ] (db_relation rel))
                in
                let new_key = List.filter key ~f:(fun k' -> Name.O.(k <> k')) in
                let new_ps =
                  List.filter ps ~f:(fun p ->
                      not ([%compare.equal: Pred.t] p (Name k)))
                  |> List.map ~f:(Pred.scoped (schema lhs) scope)
                in
                let filter_pred =
                  Binop (Eq, Name k, Name (Name.create key_name))
                  |> Pred.scoped (schema lhs) scope
                in
                let new_r =
                  replace_rel rel (filter filter_pred (db_relation rel)) r
                in
                let new_group_by =
                  if List.is_empty new_key then select new_ps new_r
                  else group_by new_ps new_key new_r
                in
                let key_scalar =
                  let p =
                    Pred.scoped (schema lhs) scope
                      (As_pred (Name (Name.create key_name), Name.name k))
                  in
                  scalar p
                in
                list lhs scope (tuple [ key_scalar; new_group_by ] Cross))
        | _ -> []);
    }
    |> run_everywhere

  let tf_elim_groupby_filter _ =
    let open A in
    {
      name = "elim-groupby-filter";
      f =
        (function
        | { node = GroupBy (ps, key, { node = Filter (p, r); _ }); _ }
          when no_params r ->
            let scope = Fresh.name fresh "s%d" in
            let key_name = Fresh.name fresh "k%d" in
            let new_key =
              List.map key ~f:(fun n ->
                  sprintf "%s_%s" key_name (Name.to_var n))
            in
            let select_list =
              List.map2_exn key new_key ~f:(fun n n' -> As_pred (Name n, n'))
            in
            let lhs = dedup (select select_list r) in
            let filter_pred =
              List.map2_exn key new_key ~f:(fun n n' ->
                  Binop (Eq, Name n, Name (Name.create n')))
              |> List.reduce_exn ~f:(fun acc p -> Binop (And, acc, p))
              |> Pred.scoped (schema lhs) scope
            in
            [ list lhs scope (select ps (filter p (filter filter_pred r))) ]
        | _ -> []);
    }
    |> run_everywhere

  (** Groupby eliminator that works when the grouping key is all direct field
     references. It computes the set of all possible keys, which is often
     simpler than computing the true set of keys. *)
  let tf_elim_groupby_approx _ =
    let open A in
    let wrap_rel r wrapper =
      let visitor =
        object
          inherit [_] V.endo

          method! visit_Relation () old r' =
            if String.(r = r'.r_name) then wrapper (relation r') else old
        end
      in
      visitor#visit_t ()
    in
    {
      name = "elim-groupby-approx";
      f =
        (function
        | { node = GroupBy (ps, key, r); _ }
          when List.for_all key ~f:(fun n -> Option.is_some (Name.rel n)) ->
            (* Create an alias for each key. *)
            let key_aliases =
              List.fold_left key
                ~init:(Map.empty (module String))
                ~f:(fun m k ->
                  let k' = Name.name k ^ Fresh.name fresh "k%d" in
                  Map.add_exn m ~key:(Name.name k) ~data:k')
            in
            (* First, group keys by their relation. *)
            let key_groups =
              List.fold_left key
                ~init:(Map.empty (module String))
                ~f:(fun m k ->
                  Map.add_multi m ~key:(Option.value_exn (Name.rel k)) ~data:k)
            in
            (* Generate the relation of unique keys for each group. *)
            let key_rels =
              Map.mapi key_groups ~f:(fun ~key:rel ~data:ks ->
                  dedup
                    (select
                       (List.map ks ~f:(fun n ->
                            As_pred
                              (Name n, Map.find_exn key_aliases (Name.name n))))
                       (db_relation rel)))
              |> Map.data
            in
            (* Join the key relations. *)
            let key_rel = List.reduce_exn key_rels ~f:(join (Bool true)) in
            (* Wrap each reference to the original relation in a filter. *)
            let r =
              Map.fold key_groups ~init:r ~f:(fun ~key:rel ~data:ks ->
                  let filter_pred =
                    List.map ks ~f:(fun n ->
                        Binop
                          ( Eq,
                            Name n,
                            Name
                              ( Map.find_exn key_aliases (Name.name n)
                              |> Name.create ) ))
                    |> List.reduce_exn ~f:(fun p p' -> Binop (And, p, p'))
                  in
                  wrap_rel rel (fun r -> (filter filter_pred r).node))
            in
            let scope = Fresh.name Global.fresh "s%d" in
            let ps = List.map ~f:(Pred.scoped (schema key_rel) scope) ps in
            [ list key_rel scope (select ps r) ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_elim_eq_filter _ =
    let open A in
    {
      name = "elim-eq-filter";
      f =
        (function
        | { node = Filter (p, r); _ } ->
            let eqs, rest =
              Pred.conjuncts p
              |> List.partition_map ~f:(function
                   | Binop (Eq, p1, p2) -> First (p1, Fresh.name fresh "k%d", p2)
                   | p -> Second p)
            in
            if List.length eqs = 0 then []
            else
              let select_list =
                List.map eqs ~f:(fun (p, k, _) -> As_pred (p, k))
              in
              let inner_filter_pred =
                List.map eqs ~f:(fun (p, k, _) ->
                    Binop (Eq, Name (Name.create k), p))
                |> and_
              in
              let key = List.map eqs ~f:(fun (_, _, p) -> p) in
              let outer_filter r =
                match rest with [] -> r | _ -> filter (and_ rest) r
              in
              let scope = Fresh.name Global.fresh "s%d" in
              let lhs = dedup (select select_list r) in
              let inner_filter_pred =
                Pred.scoped (schema lhs) scope inner_filter_pred
              in
              [
                outer_filter
                  (hash_idx lhs scope (filter inner_filter_pred r) key);
              ]
        | _ -> []);
    }
    |> run_everywhere

  (** Given a restricted parameter range, precompute a filter that depends on a
     single table field. If the parameter is outside the range, then run the
     original filter. Otherwise, check the precomputed evidence. *)
  let tf_precompute_filter args =
    let open A in
    let field, values =
      match args with
      | f :: vs -> (A.name_of_string_exn f, List.map vs ~f:Pred.of_string_exn)
      | _ ->
          Error.create "Unexpected argument list." args [%sexp_of: string list]
          |> Error.raise
    in
    let exception Failed of Error.t in
    let run_exn r =
      match r.node with
      | Filter (p, r') ->
          let schema = schema r' in
          let free_vars =
            Set.diff (Free.pred_free p) (Set.of_list (module Name) schema)
            |> Set.to_list
          in
          let free_var =
            match free_vars with
            | [ v ] -> v
            | _ ->
                let err =
                  Error.of_string
                    "Unexpected number of free variables in predicate."
                in
                raise (Failed err)
          in
          let encoder =
            List.foldi values ~init:(Int 0) ~f:(fun i else_ v ->
                let witness =
                  Pred.subst (Map.singleton (module Name) free_var v) p
                in
                If (witness, Int (i + 1), else_))
          in
          let decoder =
            List.foldi values ~init:(Int 0) ~f:(fun i else_ v ->
                If (Binop (Eq, Name free_var, v), Int (i + 1), else_))
          in
          let fresh_name = Fresh.name fresh "p%d_" ^ Name.name field in
          let select_list =
            As_pred (encoder, fresh_name)
            :: List.map schema ~f:(fun n -> Name n)
          in
          [
            filter
              (If
                 ( Binop (Eq, decoder, Int 0),
                   p,
                   Binop (Eq, decoder, Name (Name.create fresh_name)) ))
              (select select_list r');
          ]
      | _ -> []
    in
    let f r = try run_exn r with Failed _ -> [] in
    { name = "precompute-filter"; f } |> run_everywhere

  let tf_precompute_filter_bv args =
    let open A in
    let values = List.map args ~f:Pred.of_string_exn in
    let exception Failed of Error.t in
    let run_exn r =
      match r.node with
      | Filter (p, r') ->
          let schema = schema r' in
          let free_vars =
            Set.diff (Free.pred_free p) (Set.of_list (module Name) schema)
            |> Set.to_list
          in
          let free_var =
            match free_vars with
            | [ v ] -> v
            | _ ->
                let err =
                  Error.of_string
                    "Unexpected number of free variables in predicate."
                in
                raise (Failed err)
          in
          let witness_name = Fresh.name fresh "wit%d_" in
          let witnesses =
            List.mapi values ~f:(fun i v ->
                As_pred
                  ( Pred.subst (Map.singleton (module Name) free_var v) p,
                    sprintf "%s_%d" witness_name i ))
          in
          let filter_pred =
            List.foldi values ~init:p ~f:(fun i else_ v ->
                If
                  ( Binop (Eq, Name free_var, v),
                    Name (Name.create (sprintf "%s_%d" witness_name i)),
                    else_ ))
          in
          let select_list = witnesses @ List.map schema ~f:(fun n -> Name n) in
          [ filter filter_pred (select select_list r') ]
      | _ -> []
    in
    let f r = try run_exn r with Failed _ -> [] in
    { name = "precompute-filter-bv"; f } |> run_everywhere

  let gen_ordered_idx ?lb ?ub p r =
    let open A in
    let t = Pred.to_type p in
    let default_min =
      let open Prim_type in
      match t with
      | IntT _ -> Int (Int.min_value + 1)
      | FixedT _ -> Fixed Fixed_point.(min_value + of_int 1)
      | DateT _ -> Date (Date.of_string "0000-01-01")
      | _ -> failwith "Unexpected type."
    in
    let default_max =
      let open Prim_type in
      match t with
      | IntT _ -> Int Int.max_value
      | FixedT _ -> Fixed Fixed_point.max_value
      | DateT _ -> Date (Date.of_string "9999-01-01")
      | _ -> failwith "Unexpected type."
    in
    let fix_upper_bound bound kind =
      match kind with
      | `Open -> Ok bound
      | `Closed -> (
          let open Prim_type in
          match t with
          | IntT _ -> Ok (Binop (Add, bound, Int 1))
          | DateT _ -> Ok (Binop (Add, bound, Unop (Day, Int 1)))
          | FixedT _ -> Error "No open inequalities with fixed."
          | NullT | StringT _ | BoolT _ | TupleT _ | VoidT ->
              failwith "Unexpected type." )
    in
    let fix_lower_bound bound kind =
      match kind with
      | `Closed -> Ok bound
      | `Open -> (
          let open Prim_type in
          match t with
          | IntT _ -> Ok (Binop (Add, bound, Int 1))
          | DateT _ -> Ok (Binop (Add, bound, Unop (Day, Int 1)))
          | FixedT _ -> Error "No open inequalities with fixed."
          | NullT | StringT _ | BoolT _ | TupleT _ | VoidT ->
              failwith "Unexpected type." )
    in
    let open Result.Let_syntax in
    let%bind lb =
      match lb with
      | None -> Ok default_min
      | Some (b, k) -> fix_lower_bound b k
    in
    let%map ub =
      match ub with
      | None -> Ok default_max
      | Some (b, k) -> fix_upper_bound b k
    in
    let k = Fresh.name fresh "k%d" in
    let select_list = [ As_pred (p, k) ] in
    let filter_pred = Binop (Eq, Name (Name.create k), p) in
    let scope = Fresh.name Global.fresh "s%d" in
    let lhs = dedup (select select_list r) in
    ordered_idx lhs scope
      (filter (Pred.scoped (schema lhs) scope filter_pred) r)
      [ (Some (lb, `Closed), Some (ub, `Open)) ]

  let tf_elim_cmp_filter _ =
    let result_to_list = function Ok x -> [ x ] | Error _ -> [] in
    {
      name = "elim-cmp-filter";
      f =
        (fun r ->
          match r with
          | {
           node = Filter (Binop (And, Binop (Ge, p, lb), Binop (Lt, p', ub)), r);
           _;
          }
            when [%compare.equal: Pred.t] p p' ->
              gen_ordered_idx ~lb:(lb, `Closed) ~ub:(ub, `Open) p r
              |> result_to_list
          | { node = Filter (Binop (Ge, p', p), r); _ }
          | { node = Filter (Binop (Le, p, p'), r); _ } ->
              (gen_ordered_idx ~ub:(p', `Closed) p r |> result_to_list)
              @ (gen_ordered_idx ~lb:(p, `Closed) p' r |> result_to_list)
          | { node = Filter (Binop (Lt, p, p'), r); _ }
          | { node = Filter (Binop (Gt, p', p), r); _ } ->
              (gen_ordered_idx ~ub:(p', `Open) p r |> result_to_list)
              @ (gen_ordered_idx ~lb:(p, `Open) p' r |> result_to_list)
          | _ -> []);
    }
    |> run_everywhere

  (* let tf_elim_cmp_filter_simple _ =
   *   let open A in
   *   { name= "elim-cmp-filter"
   *   ; f=
   *       (function
   *       | { node=
   *             Filter
   *               (Binop (And, Binop (Ge, Name n1, lb), Binop (Lt, Name n2, ub)), r); _
   *         } ->
   *           if Name.(n1 = n2) then
   *             let rel =
   *               match (n1.relation, n2.relation) with
   *               | Some r1, Some r2 -> if String.(r1 = r2) then Some r1 else None
   *               | _ -> None
   *             in
   *             match rel with
   *             | Some rel -> gen_ordered_idx ~lb ~ub (Name n1) (relation rel)
   *             | None -> []
   *           else []
   *       | {node= Filter (Binop (Lt, p, Name), r); _}
   *       | {node= Filter (Binop (Le, p, p'), r); _}
   *        |{node= Filter (Binop (Lt, p, p'), r); _}
   *        |{node= Filter (Binop (Ge, p', p), r); _}
   *        |{node= Filter (Binop (Gt, p', p), r); _} ->
   *           gen ~ub:p' p r @ gen ~lb:p p' r
   *       | _ -> []) }
   *   |> run_everywhere *)

  let same_orders r1 r2 =
    [%compare.equal: (Pred.t * order) list] (A.order_of r1) (A.order_of r2)

  let orderby_list key { l_keys = r1; l_scope; l_values = r2 } =
    let open A in
    let schema1 = Schema.schema r1 |> Schema.scoped l_scope in

    let eqs = Equiv.eqs r2 |> Set.to_list in
    let names =
      List.concat_map eqs ~f:(fun (n, n') -> [ n; n' ])
      @ List.filter_map ~f:(fun (p, _) -> Pred.to_name p) key
      @ schema1
    in
    (* Create map from names to sets of equal names. *)
    let eq_map =
      names
      |> List.dedup_and_sort ~compare:[%compare: Name.t]
      |> List.map ~f:(fun n -> (n, Union_find.create n))
      |> Hashtbl.of_alist_exn (module Name)
    in
    (* Add known equalities. *)
    List.iter eqs ~f:(fun (n, n') ->
        let s = Hashtbl.find_exn eq_map n in
        let s' = Hashtbl.find_exn eq_map n' in
        Union_find.union s s');
    let exception No_key in
    try
      let new_key =
        List.map key ~f:(fun (p, o) ->
            let p' =
              match Pred.to_name p with
              | Some n -> (
                  let s = Hashtbl.find_exn eq_map n in
                  (* Find an equivalent name in schema 1. *)
                  let n' =
                    List.find schema1 ~f:(fun n' ->
                        let s' = Hashtbl.find_exn eq_map n' in
                        Union_find.same_class s s')
                  in
                  match n' with Some n' -> Name n' | None -> raise No_key )
              | None -> raise No_key
            in
            (p', o))
      in
      let scope = Fresh.name Global.fresh "s%d" in
      [ list (order_by new_key r1) scope r2 ]
    with No_key -> []

  let orderby_cross_tuple key rs =
    let open A in
    match rs with
    | r :: rs ->
        let schema = schema r in
        let sschema = Set.of_list (module Name) schema in
        let skey =
          Set.of_list
            (module Name)
            (List.filter_map ~f:(fun (p, _) -> Pred.to_name p) key)
        in
        if Set.is_subset skey ~of_:sschema then
          [ tuple (order_by key r :: rs) Cross ]
        else []
    | _ -> []

  let tf_push_orderby _ =
    let open A in
    {
      name = "push-orderby";
      f =
        (fun r ->
          let rs =
            match r with
            | { node = OrderBy { key; rel = { node = Select (ps, r); _ } }; _ }
              ->
                [ select ps (order_by key r) ]
            | { node = OrderBy { key; rel = { node = Filter (ps, r); _ } }; _ }
              ->
                [ filter ps (order_by key r) ]
            | { node = OrderBy { key; rel = { node = AList l; _ } }; _ } ->
                (* If we order a lists keys then the keys will be ordered in the
                   list. *)
                orderby_list key l
            | {
             node = OrderBy { key; rel = { node = ATuple (rs, Cross); _ } };
             _;
            } ->
                orderby_cross_tuple key rs
            | _ -> []
          in
          List.filter rs ~f:(same_orders r));
    }
    |> run_everywhere

  let tf_push_select _ =
    (* Generate aggregates for collections that act by concatenating their children. *)
    let gen_concat_select_list outer_preds inner_schema =
      let visitor =
        object (self : 'a)
          inherit [_] V.mapreduce

          inherit [_] Util.list_monoid

          method add_agg aggs a =
            match
              List.find aggs ~f:(fun (_, a') -> [%compare.equal: Pred.t] a a')
            with
            | Some (n, _) -> (Name n, aggs)
            | None ->
                let n = Fresh.name fresh "agg%d" |> Name.create in
                (Name n, (n, a) :: aggs)

          method! visit_Sum aggs p =
            let n, aggs' = self#add_agg aggs (Sum p) in
            (Sum n, aggs')

          method! visit_Count aggs =
            let n, aggs' = self#add_agg aggs Count in
            (Sum n, aggs')

          method! visit_Min aggs p =
            let n, aggs' = self#add_agg aggs (Min p) in
            (Min n, aggs')

          method! visit_Max aggs p =
            let n, aggs' = self#add_agg aggs (Max p) in
            (Max n, aggs')

          method! visit_Avg aggs p =
            let s, aggs' = self#add_agg aggs (Sum p) in
            let c, aggs'' = self#add_agg aggs' Count in
            (Binop (Div, Sum s, Sum c), aggs'')
        end
      in
      let outer_aggs, inner_aggs =
        List.fold_left outer_preds ~init:([], []) ~f:(fun (op, ip) p ->
            let p', ip' = visitor#visit_pred ip p in
            (op @ [ p' ], ip'))
      in
      let inner_aggs =
        List.map inner_aggs ~f:(fun (n, a) -> As_pred (a, Name.name n))
      in
      (* Don't want to project out anything that we might need later. *)
      let inner_fields = inner_schema |> List.map ~f:(fun n -> Name n) in
      (outer_aggs, inner_aggs @ inner_fields)
    in
    let open A in
    {
      name = "push-select";
      f =
        (fun r ->
          match r.node with
          | Select (ps, { node = AHashIdx h; _ }) ->
              let outer_preds =
                List.filter_map ps ~f:Pred.to_name
                |> List.map ~f:(fun n -> Name n)
              in
              [
                select outer_preds
                  (hash_idx' { h with hi_values = select ps h.hi_values });
              ]
          | Select (ps, { node = AOrderedIdx o; _ }) ->
              let outer_aggs, inner_aggs =
                gen_concat_select_list ps (schema o.oi_values)
              in
              [
                select outer_aggs
                  (ordered_idx'
                     { o with oi_values = select inner_aggs o.oi_values });
              ]
          | Select (ps, { node = AList l; _ }) ->
              let outer_aggs, inner_aggs =
                gen_concat_select_list ps (schema l.l_values)
              in
              [
                select outer_aggs
                  (list' { l with l_values = select inner_aggs l.l_values });
              ]
          | Select (ps, { node = ATuple (r' :: rs', Concat); _ }) ->
              let outer_aggs, inner_aggs =
                gen_concat_select_list ps (schema r')
              in
              [
                select outer_aggs
                  (tuple (List.map (r' :: rs') ~f:(select inner_aggs)) Concat);
              ]
          | _ -> []);
    }
    |> run_everywhere

  let tf_split_select _ =
    let open A in
    let gen_select_list outer_preds inner_rel =
      let visitor =
        object (self : 'a)
          inherit [_] V.mapreduce

          inherit [_] Util.list_monoid

          method add_agg aggs a =
            match
              List.find aggs ~f:(fun (_, a') -> [%compare.equal: Pred.t] a a')
            with
            | Some (n, _) -> (Name n, aggs)
            | None ->
                let n = Fresh.name fresh "agg%d" |> Name.create in
                (Name n, (n, a) :: aggs)

          method! visit_Sum aggs p =
            let n, aggs' = self#add_agg aggs p in
            (Sum n, aggs')

          method! visit_Min aggs p =
            let n, aggs' = self#add_agg aggs p in
            (Min n, aggs')

          method! visit_Max aggs p =
            let n, aggs' = self#add_agg aggs p in
            (Max n, aggs')

          method! visit_Avg aggs p =
            let n, aggs' = self#add_agg aggs p in
            (Avg n, aggs')
        end
      in
      let outer_aggs, inner_aggs =
        List.fold_left outer_preds ~init:([], []) ~f:(fun (op, ip) p ->
            let p', ip' = visitor#visit_pred ip p in
            (op @ [ p' ], ip'))
      in
      let inner_aggs =
        List.map inner_aggs ~f:(fun (n, a) -> As_pred (a, Name.name n))
      in
      (* Don't want to project out anything that we might need later. *)
      let inner_fields = schema inner_rel |> List.map ~f:(fun n -> Name n) in
      (outer_aggs, inner_aggs @ inner_fields)
    in
    {
      name = "split-select";
      f =
        (fun r ->
          match r.node with
          | Select (ps, r') ->
              let outer_aggs, inner_aggs = gen_select_list ps r' in
              [ select outer_aggs (select inner_aggs r') ]
          | _ -> []);
    }
    |> run_everywhere

  (** Check that a predicate is applied to a schema that has the right fields. *)
  let predicate_is_valid p s =
    let visitor =
      object
        inherit [_] V.reduce

        method zero = true

        method plus = ( && )

        method! visit_Exists () _ =
          (* TODO: Not correct. Account for parameters. *)
          true

        method! visit_First () _ =
          (* TODO: Not correct. Account for parameters. *)
          true

        method! visit_Name () n =
          let is_param = Option.is_none (Name.rel n) in
          let in_schema = List.mem s n ~equal:[%compare.equal: Name.t] in
          let is_valid = is_param || in_schema in
          if not is_valid then
            Logs.debug (fun m -> m "Predicate not valid. %a missing." Name.pp n);
          is_valid
      end
    in
    visitor#visit_pred () p

  (** Extend a select operator to emit all of the fields in with_. Returns the
     new select list. *)
  let extend_select ~with_ ps r =
    let needed_fields =
      let of_list = Set.of_list (module Name) in
      (* These are the fields that are emitted by r, used in with_ and not
         exposed already by ps. *)
      Set.diff
        (Set.inter with_ (of_list (schema r)))
        (of_list (List.filter_map ~f:Pred.to_name ps))
      |> Set.to_list
      |> List.map ~f:(fun n -> Name n)
    in
    ps @ needed_fields

  let hoist_filter_if ~f r =
    let open A in
    let ret =
      match r.node with
      | OrderBy { key; rel = { node = Filter (p, r); _ } } ->
          [ (p, order_by key r) ]
      | GroupBy (ps, key, { node = Filter (p, r); _ }) ->
          [ (p, group_by ps key r) ]
      | Filter (p, { node = Filter (p', r); _ }) -> [ (p', filter p r) ]
      | Select (ps, { node = Filter (p, r); _ }) ->
          [ (p, select (extend_select ps r ~with_:(Free.pred_free p)) r) ]
      | Join { pred; r1 = { node = Filter (p, r); _ }; r2 } ->
          [ (p, join pred r r2) ]
      | Join { pred; r1; r2 = { node = Filter (p, r); _ } } ->
          [ (p, join pred r1 r) ]
      | Dedup { node = Filter (p, r); _ } -> [ (p, dedup r) ]
      | _ -> []
    in
    List.filter ret ~f
    |> List.filter_map ~f:(fun (p, r) ->
           let schema = schema r in
           if predicate_is_valid p schema then Some (filter p r)
           else (
             Logs.debug (fun m ->
                 m "Cannot hoist: %a" Sexp.pp_hum
                   ([%sexp_of: Name.t list] schema));
             None ))

  let tf_hoist_orderby _ =
    let f r =
      let open A in
      match r.node with
      | Select (ps, { node = OrderBy { key; rel }; _ }) ->
          [
            order_by key
              (select
                 (extend_select ps rel
                    ~with_:
                      ( List.map key ~f:(fun (p, _) -> Free.pred_free p)
                      |> Set.union_list (module Name) ))
                 rel);
          ]
      | _ -> []
    in
    { name = "hoist-orderby"; f } |> run_everywhere

  let tf_hoist_filter _ =
    { name = "hoist-filter"; f = hoist_filter_if ~f:(fun _ -> true) }
    |> run_everywhere

  let tf_hoist_param_filter _ =
    {
      name = "hoist-param-filter";
      f =
        hoist_filter_if ~f:(fun (p, _) ->
            (object
               inherit [_] V.reduce

               inherit [_] Util.disj_monoid

               method! visit_Name () n = Set.mem Config.params n
            end)
              #visit_pred
              () p);
    }
    |> run_everywhere

  let tf_elim_join _ =
    let open A in
    {
      name = "elim-join";
      f =
        (function
        | { node = Join { pred; r1; r2 }; _ } ->
            [ tuple [ r1; filter pred r2 ] Cross ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_push_filter _ =
    let open A in
    {
      name = "push-filter";
      f =
        (function
        | { node = Filter (p, { node = Filter (p', r); _ }); _ } ->
            [ filter p' (filter p r) ]
        | { node = Filter (p, { node = Join { pred; r1; r2 }; _ }); _ } ->
            [ join (Binop (And, p, pred)) r1 r2 ]
        | { node = Filter (p, { node = AList l; _ }); _ } ->
            [
              list' { l with l_keys = filter p l.l_keys };
              list' { l with l_values = filter p l.l_values };
            ]
        | { node = Filter (p, { node = AHashIdx h; _ }); _ } ->
            [ hash_idx' { h with hi_values = filter p h.hi_values } ]
        | { node = Filter (p, { node = AOrderedIdx o; _ }); _ } ->
            [ ordered_idx' { o with oi_values = filter p o.oi_values } ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_merge_filter _ =
    let open A in
    {
      name = "merge-filter";
      f =
        (function
        | { node = Filter (p, { node = Filter (p', r); _ }); _ } ->
            [ filter (Binop (And, p, p')) r ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_split_filter _ =
    let open A in
    {
      name = "split-filter";
      f =
        (function
        | { node = Filter (Binop (And, p, p'), r); _ } ->
            [ filter p (filter p' r) ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_elim_disj_filter _ =
    let open A in
    {
      name = "elim-disj-filter";
      f =
        (function
        | { node = Filter (Binop (Or, p, p'), r); _ } ->
            [ tuple [ filter p r; filter p' r ] Concat ]
        | _ -> []);
    }
    |> run_everywhere

  let tf_project _ =
    {
      name = "project";
      f = (fun r -> [ Project.project ~params:Config.params r ]);
    }

  let tf_hoist_join_pred _ =
    let open A in
    {
      name = "hoist-join-pred";
      f =
        (function
        | { node = Join { pred; r1; r2 }; _ } ->
            [ filter pred (join (Bool true) r1 r2) ]
        | _ -> []);
    }
    |> run_everywhere

  let eq_preds r =
    let visitor =
      object
        inherit [_] V.reduce

        inherit [_] Util.list_monoid

        method! visit_Binop ps op arg1 arg2 =
          if [%compare.equal: Pred.Binop.t] op Eq then (arg1, arg2) :: ps
          else ps
      end
    in
    visitor#visit_t [] r

  let tf_partition args =
    let open A in
    let name =
      match args with
      | [ n ] -> A.name_of_string_exn n
      | _ -> failwith "Unexpected args."
    in
    let fresh_name =
      Caml.Format.sprintf "%s_%s" (Name.to_var name) (Fresh.name fresh "%d")
    in
    let scope = Fresh.name Global.fresh "s%d" in
    let rel = Name.rel_exn name in
    let lhs =
      dedup (select [ As_pred (Name name, fresh_name) ] (db_relation rel))
    in
    let pred =
      Pred.scoped (schema lhs) scope
        (Binop (Eq, Name name, Name (Name.create fresh_name)))
    in
    let filtered_rel = filter pred (db_relation rel) in
    {
      name = "partition";
      f = (fun r -> [ list lhs scope (replace_rel rel filtered_rel r) ]);
    }
    |> run_everywhere ~stage:`Run

  let tf_partition_domain args =
    let open A in
    let n_subst, n_domain =
      match args with
      | [ n_subst; n_domain ] ->
          (A.name_of_string_exn n_subst, A.name_of_string_exn n_domain)
      | _ -> failwith "Unexpected args."
    in
    let fresh_name =
      Caml.Format.sprintf "%s_%s" (Name.to_var n_subst) (Fresh.name fresh "%d")
    in
    let rel = Name.rel_exn n_domain in
    let lhs =
      dedup (select [ As_pred (Name n_domain, fresh_name) ] (db_relation rel))
    in
    let scope = Fresh.name Global.fresh "s%d" in
    {
      name = "partition-domain";
      f =
        (fun r ->
          [
            hash_idx lhs scope
              (subst
                 (Map.singleton
                    (module Name)
                    n_subst
                    (Name (Name.create fresh_name)))
                 r)
              [ Name n_subst ];
          ]);
    }
    |> run_everywhere ~stage:`Run

  let replace_pred r p1 p2 =
    let visitor =
      object
        inherit [_] V.endo as super

        method! visit_pred () p =
          let p = super#visit_pred () p in
          if [%compare.equal: Pred.t] p p1 then p2 else p
      end
    in
    visitor#visit_t () r

  let tf_partition_eq args =
    let open A in
    let name =
      match args with
      | [ n ] -> A.name_of_string_exn n
      | _ -> failwith "Unexpected args."
    in
    let fresh_name =
      Caml.Format.sprintf "%s_%s" (Name.to_var name) (Fresh.name fresh "%d")
    in
    let rel = Name.rel_exn name in
    {
      name = "partition-eq";
      f =
        (fun r ->
          let eqs = eq_preds r in
          (* Any predicate that is compared for equality with the partition
              field is a candidate for the hash table key. *)
          let keys =
            List.filter_map eqs ~f:(fun (p1, p2) ->
                match (p1, p2) with
                | Name n, _ when String.(Name.name n = Name.name name) ->
                    Some p2
                | _, Name n when String.(Name.name n = Name.name name) ->
                    Some p1
                | _ -> None)
          in
          let lhs =
            dedup
              (select
                 [ As_pred (Name (Name.copy ~scope:None name), fresh_name) ]
                 (db_relation rel))
          in
          let scope = Fresh.name Global.fresh "s%d" in
          let pred =
            Binop
              ( Eq,
                Name (Name.copy ~scope:None name),
                Name (Name.create fresh_name) )
            |> Pred.scoped (schema lhs) scope
          in
          let filtered_rel = filter pred (db_relation rel) in
          List.map keys ~f:(fun k ->
              (* The predicate that we chose as the key can be replaced by
                 `fresh_name`. *)
              hash_idx lhs scope
                (replace_pred
                   (replace_rel rel filtered_rel r)
                   k
                   (Name (Name.create ~scope fresh_name)))
                [ k ]));
    }
    |> run_everywhere ~stage:`Run

  let tf_split_out args =
    let open A in
    let rel, pk =
      match args with
      | [ x; y ] -> (x, A.name_of_string_exn y)
      | _ ->
          Error.create "Unexpected args." args [%sexp_of: string list]
          |> Error.raise
    in
    let rel_schema = schema (db_relation rel) in
    let eq = [%compare.equal: Name.t] in
    {
      name = "split-out";
      f =
        (fun r ->
          let schema = schema r in
          if List.mem schema pk ~equal:eq then
            let pk_rel = Option.value_exn (Name.rel pk) in
            let rel_fresh_k = sprintf "%s_%s" pk_rel (Fresh.name fresh "%d") in
            let pk_k_name = Fresh.name fresh "x%d" in
            let pk_k = Name.copy pk ~scope:(Some rel_fresh_k) in
            let rel_fresh_v = sprintf "%s_%s" pk_rel (Fresh.name fresh "%d") in
            let pk_v = Name.copy pk ~scope:(Some rel_fresh_v) in
            (* Partition the schema of the original layout between the two new layouts. *)
            let fst_sel_list, snd_sel_list =
              List.partition_tf schema ~f:(fun n ->
                  eq n pk
                  || not
                       (List.mem rel_schema n ~equal:(fun n n' ->
                            String.(Name.(name n = name n')))))
            in
            let fst_sel_list = List.map ~f:(fun n -> Name n) fst_sel_list in
            let snd_sel_list =
              List.map
                ~f:(fun n -> Name (Name.copy n ~scope:(Some rel_fresh_v)))
                snd_sel_list
            in
            [
              tuple
                [
                  select fst_sel_list r;
                  hash_idx
                    (dedup
                       (select
                          [ As_pred (Name pk_k, pk_k_name) ]
                          (db_relation rel)))
                    pk_rel
                    (select snd_sel_list
                       (filter
                          (Binop (Eq, Name pk_v, Name (Name.create pk_k_name)))
                          (db_relation rel)))
                    [ Name pk ];
                ]
                Cross;
            ]
          else []);
    }
    |> run_everywhere ~stage:`Run

  let tf_partition_size args =
    let open A in
    let field =
      match args with
      | [ x ] -> A.name_of_string_exn x
      | _ -> failwith "Unexpected args."
    in
    {
      name = "partition-size";
      f =
        (fun r ->
          [
            tuple
              (List.map
                 Int.
                   [ (2 ** 7) - 1; (2 ** 15) - 1; (2 ** 23) - 1; (2 ** 31) - 1 ]
                 ~f:(fun max ->
                   filter
                     (Binop
                        ( And,
                          Binop (Lt, Name field, Int max),
                          Binop (Gt, Name field, Int (-max)) ))
                     r))
              Concat;
          ]);
    }
    |> run_everywhere ~stage:`Run

  (* let tf_hoist_pred_constant _ =
   *   let open A in
   *   let f r =
   *     M.annotate_schema r ;
   *     annotate_free r ;
   *     let consts =
   *       match r.node with
   *       | Filter (p, r') -> Pred.constants Meta.(find_exn r' schema) p
   *       | Select (ps, r') ->
   *           List.concat_map ps ~f:(Pred.constants Meta.(find_exn r' schema))
   *           |> List.dedup_and_sort ~compare:[%compare: pred]
   *       | _ -> []
   *     in
   *     let fresh_id = Fresh.name fresh "const%d" in
   *     let fresh_name = Name.create fresh_id in
   *     List.map consts ~f:(fun p ->
   *         tuple
   *           [scalar (As_pred (p, fresh_id)); Pred.subst_single p (Name fresh_name) r]
   *           Cross )
   *   in
   *   {name= "hoist-pred-const"; f} |> run_everywhere ~stage:`Run *)

  let tf_hoist_param _ =
    let open A in
    let f r =
      match r.node with
      | AScalar
          (As_pred
            ( First { node = Select ([ As_pred (Binop (op, p1, p2), _) ], r); _ },
              n )) ->
          let fresh_id = Fresh.name fresh "const%d" in
          [
            select
              [ As_pred (Binop (op, Name (Name.create fresh_id), p2), n) ]
              (scalar (As_pred (First (select [ p1 ] r), fresh_id)));
          ]
      | _ -> []
    in
    { name = "hoist-param"; f } |> run_everywhere ~stage:`Run

  let tf_approx_dedup _ =
    let open A in
    let f r =
      match r.node with
      | Dedup { node = Select ([ As_pred (Name n, n') ], _); _ } -> (
          match Name.rel n with
          | Some r ->
              [ dedup (select [ As_pred (Name n, n') ] (db_relation r)) ]
          | None -> [] )
      | _ -> []
    in
    { name = "approx-dedup"; f } |> run_everywhere ~stage:`Both

  let is_correlated subquery input_schema =
    not
      ( Set.is_empty
      @@ Set.inter subquery.meta#free (Set.of_list (module Name) input_schema)
      )

  let tf_elim_subquery _ =
    let open A in
    let f r_main =
      let r_main = Free.annotate r_main in
      match r_main.node with
      | Filter (pred, r) ->
          let schema = Schema.schema r in
          (* Mapping from uncorrelated subqueries to unique names. *)
          let query_names =
            (object (self)
               inherit [_] V.reduce

               inherit [_] Util.list_monoid

               method! visit_t xs query =
                 if is_correlated query schema then xs
                 else
                   let result_type =
                     match Schema.schema query with
                     | [ n ] -> Name.type_exn n
                     | _ -> failwith "Unexpected schema."
                   in
                   let result_name = Fresh.name fresh "x%d" in
                   self#plus xs
                     [ (Name.create ~type_:result_type result_name, query) ]
            end)
              #visit_pred
              [] pred
          in
          let subst_query ~for_:r ~in_:outer_pred new_pred =
            (object
               inherit [_] V.endo

               method! visit_Exists () p' r' =
                 if [%compare.equal: Ast.t] r r' then new_pred else p'

               method! visit_First () p' r' =
                 if [%compare.equal: Ast.t] r r' then new_pred else p'
            end)
              #visit_pred
              () outer_pred
          in
          let pred = normal_meta_pred pred in
          let r = strip_meta r in
          (* For each subquery, generate a join *)
          List.map query_names ~f:(fun (n, q) ->
              let q = strip_meta q in
              let select_list =
                Schema.schema q
                |> List.map ~f:(fun n' -> As_pred (Name n', Name.name n))
              in
              join
                (subst_query ~for_:q ~in_:pred (Name n))
                r (select select_list q))
      | _ -> []
    in
    { name = "elim-subquery"; f } |> run_everywhere ~stage:`Both

  (* let tf_to_cnf _ =
   *   let to_boolean p = match p with
   *     | Binop (And, p1, p2) ->
   *     | Binop (Or, p1, p2) -> begin match to_boolean p1, to_boolean p2 with
   *         | `Or p1s, `Or p2s -> `Or (p1s @ p2s)
   *         | 
   *     | Unop (Not, p) -> `Not p
   *     | p -> `Atom p *)

  let transforms =
    [
      ("hoist-join-pred", tf_hoist_join_pred);
      ("elim-groupby", tf_elim_groupby);
      ("elim-groupby-partial", tf_elim_groupby_partial);
      ("elim-groupby-filter", tf_elim_groupby_filter);
      ("elim-groupby-approx", tf_elim_groupby_approx);
      ("push-orderby", tf_push_orderby);
      ("hoist-filter", tf_hoist_filter);
      ("push-filter", tf_push_filter);
      ("merge-filter", tf_merge_filter);
      ("split-filter", tf_split_filter);
      ("elim-disj-filter", tf_elim_disj_filter);
      ("elim-eq-filter", tf_elim_eq_filter);
      ("elim-cmp-filter", tf_elim_cmp_filter);
      ("elim-join", tf_elim_join);
      ("row-store", tf_row_store);
      ("project", tf_project);
      ("push-select", tf_push_select);
      ("split-select", tf_split_select);
      ("partition", tf_partition);
      ("partition-eq", tf_partition_eq);
      ("partition-size", tf_partition_size);
      ("partition-domain", tf_partition_domain);
      ("split-out", tf_split_out)
      (* ; ("hoist-pred-const", tf_hoist_pred_constant) *);
      ("hoist-param", tf_hoist_param);
      ("precompute-filter", tf_precompute_filter);
      ("precompute-filter-bv", tf_precompute_filter_bv);
      ("approx-dedup", tf_approx_dedup);
      ("hoist-param-filter", tf_hoist_param_filter);
      ("elim-subquery", tf_elim_subquery);
      ("hoist-orderby", tf_hoist_orderby);
    ]

  let of_string_exn s =
    let tf_strs =
      try
        Transform_parser.transforms_eof Transform_lexer.token
          (Lexing.from_string s)
      with Parser_utils.ParseError (msg, line, col) as e ->
        Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col);
        raise e
    in
    List.map tf_strs ~f:(fun (name, args, index) ->
        let _, tf_gen =
          List.find_exn transforms ~f:(fun (n, _) -> String.(name = n))
        in
        let tf = tf_gen args in
        match index with
        | Some i -> { tf with f = (fun r -> [ List.nth_exn (run tf r) i ]) }
        | None -> { tf with f = (fun r -> run tf r) })
end
