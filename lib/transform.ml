open Collections
open Ast
open Abslayout
open Schema
module R = Resolve

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Simplify_tactic.Config.S

    include Filter_tactics.Config.S

    include Simple_tactics.Config.S

    include Join_opt.Config.S

    include Select_tactics.Config.S

    include Groupby_tactics.Config.S

    include Join_elim_tactics.Config.S

    include Type_cost.Config.S

    val cost_timeout : float option
  end
end

module Make (Config : Config.S) = struct
  open Config
  module O = Ops.Make (Config)
  open O
  module Sf = Simplify_tactic.Make (Config)
  module F = Filter_tactics.Make (Config)
  module S = Simple_tactics.Make (Config)
  module Join_opt = Join_opt.Make (Config)
  module Simplify_tactic = Simplify_tactic.Make (Config)
  module Select_tactics = Select_tactics.Make (Config)
  module Groupby_tactics = Groupby_tactics.Make (Config)
  module Join_elim_tactics = Join_elim_tactics.Make (Config)
  module Tactics_util = Tactics_util.Make (Config)
  module Dedup_tactics = Dedup_tactics.Make (Config)

  let project r = Some (r |> Resolve.resolve ~params |> Project.project_once)

  let project = of_func project ~name:"project"

  module Config = struct
    include Config

    let simplify =
      let tf = fix (seq_many [ project; Sf.simplify ]) in
      Some (fun r -> Option.value (apply tf Path.root r) ~default:r)
  end

  module Cost = Type_cost.Make (Config)

  let is_serializable r p =
    let r = Resolve.resolve ~params r in
    Path.get_exn p r |> is_serializeable |> Result.is_ok

  let has_params r p = Path.get_exn p r |> free |> overlaps params

  let has_free r p = not (Set.is_empty (free (Path.get_exn p r)))

  let key_is_supported r key =
    let s = Set.of_list (module Name) (schema r) in
    List.for_all key ~f:(fun (p, _) ->
        Tactics_util.is_supported r.meta#stage s p)

  let push_orderby r =
    let module C = (val Constructors.Annot.with_strip_meta (fun () -> ())) in
    let open C in
    let orderby_cross_tuple key rs =
      match rs with
      | r :: rs ->
          if key_is_supported r key then
            Some (tuple (order_by key r :: List.map ~f:strip_meta rs) Cross)
          else None
      | _ -> None
    in
    let orderby_list key r1 r2 =
      let scope = scope_exn r1 in
      let r1 = strip_scope r1 in
      if key_is_supported r1 key then Some (list (order_by key r1) scope r2)
      else None
    in
    match r.node with
    | OrderBy { key; rel = { node = Select (ps, r); _ } } ->
        if key_is_supported r key then Some (select ps (order_by key r))
        else None
    | OrderBy { key; rel = { node = Filter (ps, r); _ } } ->
        Some (filter ps (order_by key r))
    | OrderBy { key; rel = { node = AHashIdx h; _ } } ->
        Some
          (hash_idx ?key_layout:h.hi_key_layout h.hi_keys h.hi_scope
             (order_by key h.hi_values) h.hi_lookup)
    | OrderBy { key; rel = { node = AList (r1, r2); _ } } ->
        (* If we order a list's keys then the keys will be ordered in the list. *)
        orderby_list key r1 r2
    | OrderBy { key; rel = { node = ATuple (rs, Cross); _ } } ->
        orderby_cross_tuple key rs
    | OrderBy { key; rel = { node = DepJoin d; _ } } ->
        if key_is_supported d.d_lhs key then
          Some (dep_join (order_by key d.d_lhs) d.d_alias d.d_rhs)
        else None
    | _ -> None

  let push_orderby =
    of_func_pre push_orderby ~name:"push-orderby" ~pre:(Resolve.resolve ~params)

  (* Recursively optimize subqueries. *)
  let apply_to_subqueries tf =
    let f r =
      let visitor =
        object (self : 'a)
          inherit [_] map

          method visit_subquery r =
            Option.value_exn ~message:"Transforming subquery failed."
              (O.apply tf Path.root r)

          method! visit_Exists () r = Exists (self#visit_subquery r)

          method! visit_First () r = First (self#visit_subquery r)
        end
      in
      Some (visitor#visit_t () r)
    in
    of_func f ~name:"apply-to-subqueries"

  let push_all_unparameterized_filters =
    fix (for_all F.push_filter Path.(all >>? is_run_time >>? is_filter))

  let hoist_all_filters =
    fix (for_all F.hoist_filter (Path.all >>? is_filter >> parent))

  let elim_param_filter tf test =
    (* Eliminate comparison filters. *)
    fix
      (seq_many
         [
           (* Hoist parameterized filters as far up as possible. *)
           fix (for_all F.hoist_filter (Path.all >>? is_param_filter >> parent));
           Branching.(
             seq_many
               [
                 unroll_fix
                   (O.at_ F.push_filter
                      Path.(all >>? test >>? is_run_time >>| shallowest));
                 (* Eliminate a comparison filter. *)
                 choose (for_all tf Path.(all >>? test >>? is_run_time)) id;
                 lift
                   (O.seq_many
                      [
                        push_all_unparameterized_filters;
                        O.for_all S.row_store
                          Path.(all >>? is_run_time >>? is_relation);
                        push_all_unparameterized_filters;
                        fix project;
                        Simplify_tactic.simplify;
                      ]);
               ]
             |> lower (min Cost.cost));
         ])

  let try_partition tf =
    Branching.(
      seq_many [ choose F.partition id; lift tf ] |> lower (min Cost.cost))

  let try_ tf rest =
    Branching.(seq (choose (lift tf) id) (lift rest) |> lower (min Cost.cost))

  let try_many tfs rest =
    Branching.(
      seq (choose_many (List.map ~f:lift tfs)) (lift rest)
      |> lower (min Cost.cost))

  let is_serializable' r =
    let bad_runtime_op =
      Path.(
        all >>? is_run_time
        >>? Infix.(
              is_join || is_groupby || is_orderby || is_dedup || is_relation))
        r
      |> Seq.is_empty |> not
    in
    let mis_bound_params =
      Path.(all >>? is_compile_time) r
      |> Seq.for_all ~f:(fun p ->
             not (overlaps (free (Path.get_exn p r)) params))
      |> not
    in
    if bad_runtime_op then Error (Error.of_string "Bad runtime operation.")
    else if mis_bound_params then
      Error (Error.of_string "Parameters referenced at compile time.")
    else Ok ()

  let is_serializable'' r = Result.is_ok @@ is_serializable' r

  let opt =
    let open Infix in
    seq_many
      [
        (* Eliminate groupby operators. *)
        traced ~name:"elim-groupby"
        @@ fix
        @@ at_ Groupby_tactics.elim_groupby
             (Path.all >>? is_groupby >>| shallowest);
        (* Hoist parameterized filters as far up as possible. *)
        traced ~name:"hoist-param-filters"
        @@ fix
        @@ at_ F.hoist_filter
             (Path.all >>? is_param_filter >>| deepest >>= parent)
        (* Eliminate unparameterized join nests. Try using join optimization and
           using a simple row store. *);
        traced ~name:"elim-join-nests"
        @@ try_many
             [
               at_ Join_opt.transform
                 Path.(
                   all >>? is_join >>? is_run_time >>? not has_free
                   >>| shallowest);
               at_ S.row_store
                 Path.(
                   all >>? is_join >>? is_run_time >>? not has_free
                   >>| shallowest);
               id;
             ]
             (seq_many
                [
                  push_all_unparameterized_filters;
                  project;
                  traced ~name:"elim-join-filter"
                  @@ at_ Join_elim_tactics.elim_join_filter
                       (Path.all >>? is_join >>| shallowest);
                  try_
                    (traced (first F.elim_disjunct (Path.all >>? is_filter)))
                    (seq_many
                       [
                         (* Push constant filters *)
                         traced ~name:"push-constant-filters"
                         @@ fix
                         @@ at_ F.push_filter
                              Castor.Path.(
                                all >>? is_const_filter >>| shallowest);
                         (* Push orderby operators into compile time position if possible. *)
                         traced ~name:"push-orderby"
                         @@ fix
                         @@ at_ push_orderby
                              Path.(
                                all >>? is_orderby >>? is_run_time
                                >>| shallowest);
                         (* Eliminate comparison filters. *)
                         traced ~name:"elim-cmp-filters"
                         @@ elim_param_filter F.elim_cmp_filter
                              is_param_cmp_filter;
                         (* Eliminate the deepest equality filter. *)
                         traced ~name:"elim-deepest-eq-filter"
                         @@ elim_param_filter
                              (Branching.lift F.elim_eq_filter)
                              is_param_filter;
                         traced ~name:"push-all-unparam-filters"
                         @@ push_all_unparameterized_filters;
                         (* Eliminate all unparameterized relations. *)
                         traced ~name:"elim-unparam-relations"
                         @@ fix
                         @@ seq_many
                              [
                                at_ S.row_store
                                  Path.(
                                    all >>? is_run_time >>? not has_params
                                    >>? not is_serializable
                                    >>? not (contains is_collection)
                                    >>| shallowest);
                                push_all_unparameterized_filters;
                              ];
                         traced ~name:"push-all-unparam-filters"
                         @@ push_all_unparameterized_filters;
                         (* Push selections above collections. *)
                         traced ~name:"push-select-above-collection"
                         @@ fix
                         @@ for_all Select_tactics.push_select
                              Path.(
                                all >>? is_select >>? is_run_time
                                >>? above is_collection);
                         (* Push orderby operators into compile time position if possible. *)
                         traced ~name:"push-orderby-into-ctime"
                         @@ fix
                         @@ at_ push_orderby
                              Path.(
                                all >>? is_orderby >>? is_run_time
                                >>| shallowest)
                         (* Last-ditch tactic to eliminate orderby. *);
                         traced ~name:"final-orderby-elim"
                         @@ for_all S.row_store
                              Path.(all >>? is_orderby >>? is_run_time);
                         (* Try throwing away structure if it reduces overall cost. *)
                         ( traced ~name:"drop-structure"
                         @@ Branching.(
                              seq_many
                                [
                                  choose id
                                    (seq_many
                                       [
                                         for_all (lift S.row_store)
                                           Path.(
                                             all >>? is_run_time
                                             >>? not has_params);
                                         lift push_all_unparameterized_filters;
                                       ]);
                                  filter is_serializable;
                                ]
                              |> lower (min Cost.cost)) )
                         (* Cleanup*);
                         traced ~name:"cleanup" @@ fix
                         @@ seq_many
                              [
                                for_all Select_tactics.push_simple_select
                                  Path.(all >>? is_select);
                                for_all Dedup_tactics.push_dedup
                                  Path.(all >>? is_dedup);
                                for_all Dedup_tactics.elim_dedup
                                  Path.(all >>? is_dedup);
                              ];
                         fix project;
                         push_all_unparameterized_filters;
                         Simplify_tactic.simplify;
                         traced @@ filter is_serializable'';
                       ]);
                ]);
      ]

  let opt_toplevel =
    seq_many
      [
        traced ~name:"opt-main" @@ try_partition opt;
        traced ~name:"opt-subqueries" @@ apply_to_subqueries opt;
      ]

  let is_serializable = is_serializable'
end

let optimize (module C : Config.S) r =
  let open Option.Let_syntax in
  (* Optimize outer query. *)
  let%map r =
    let module T = Make (C) in
    let module O = Ops.Make (C) in
    O.apply T.(try_partition opt) Path.root r
  in
  (* Recursively optimize subqueries. *)
  let apply_to_subqueries r =
    let visitor =
      object (self : 'a)
        inherit [_] map

        method visit_subquery r =
          let module C = struct
            include C

            let params = Set.union params (free r)
          end in
          let module O = Ops.Make (C) in
          let module T = Make (C) in
          Option.value_exn ~message:"Transforming subquery failed."
            (O.apply T.opt Path.root r)

        method! visit_Exists () r = Exists (self#visit_subquery r)

        method! visit_First () r = First (self#visit_subquery r)

        method! visit_AList () (rk, rv) = AList (rk, self#visit_t () rv)

        method! visit_AOrderedIdx () (rk, rv, m) =
          AOrderedIdx (rk, self#visit_t () rv, m)

        method! visit_AHashIdx () h =
          AHashIdx { h with hi_values = self#visit_t () h.hi_values }

        method! visit_AScalar () v = AScalar v
      end
    in
    visitor#visit_t () r
  in
  apply_to_subqueries r
