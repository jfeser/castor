open Core
open Castor
open Collections
open Abslayout
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

module Make (Config : Config.S) () = struct
  open Config
  module O = Ops.Make (Config)
  open O
  module Sf = Simplify_tactic.Make (Config)
  module M0 = Abslayout_db.Make (Config)
  module F = Filter_tactics.Make (Config)
  module S = Simple_tactics.Make (Config)
  module Join_opt = Join_opt.Make (Config)
  module Simplify_tactic = Simplify_tactic.Make (Config)
  module Select_tactics = Select_tactics.Make (Config)
  module Groupby_tactics = Groupby_tactics.Make (Config)
  module Join_elim_tactics = Join_elim_tactics.Make (Config)
  module Tactics_util = Tactics_util.Make (Config)
  module Dedup_tactics = Dedup_tactics.Make (Config)

  let project r = Some (Project.project_once r)

  let project = of_func project ~name:"project"

  module Config = struct
    include Config

    let simplify =
      let tf = fix (seq_many [project; Sf.simplify]) in
      Some (fun r -> Option.value (apply tf Path.root r) ~default:r)
  end

  module Type_cost = Type_cost.Make (Config)

  let is_serializable r p =
    let r' = Path.get_exn p (R.resolve ~params r) in
    if is_serializeable r' then
      (* Logs.debug (fun m -> m "Is serializable: %a" Abslayout.pp r') ; *)
      true
    else
      (* Logs.debug (fun m -> m "Is not serializable: %a" Abslayout.pp r') ; *)
      false

  let has_params r p =
    let r' = R.resolve ~params r in
    let r' = Path.get_exn p r' in
    overlaps (free r') params

  let has_free r p = not (Set.is_empty (free (Path.get_exn p r)))

  let push_orderby r =
    let key_is_supported r key =
      let s = Set.of_list (module Name) (schema_exn r) in
      List.for_all key ~f:(fun (p, _) -> Tactics_util.is_supported s p)
    in
    let orderby_cross_tuple key rs =
      match rs with
      | r :: rs ->
          if key_is_supported r key then
            Some (tuple (order_by key r :: rs) Cross)
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
    | OrderBy {key; rel= {node= Select (ps, r); _}} ->
        let s = Set.of_list (module Name) (schema_exn r) in
        if List.for_all key ~f:(fun (p, _) -> Tactics_util.is_supported s p)
        then Some (select ps (order_by key r))
        else None
    | OrderBy {key; rel= {node= Filter (ps, r); _}} ->
        Some (filter ps (order_by key r))
    | OrderBy {key; rel= {node= AHashIdx h; _}} ->
        Some (hash_idx' {h with hi_values= order_by key h.hi_values})
    | OrderBy {key; rel= {node= AList (r1, r2); _}} ->
        (* If we order a lists keys then the keys will be ordered in the
                   list. *)
        orderby_list key r1 r2
    | OrderBy {key; rel= {node= ATuple (rs, Cross); _}} ->
        orderby_cross_tuple key rs
    | OrderBy {key; rel= {node= DepJoin d; _}} ->
        if key_is_supported d.d_lhs key then
          Some (dep_join' {d with d_lhs= order_by key d.d_lhs})
        else None
    | _ -> None

  let push_orderby = of_func push_orderby ~name:"push-orderby"

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
         [ (* Hoist parameterized filters as far up as possible. *)
           fix
             (for_all F.hoist_filter (Path.all >>? is_param_filter >> parent))
         ; Branching.(
             seq_many
               [ unroll_fix
                   (O.at_ F.push_filter
                      Path.(all >>? test >>? is_run_time >>| shallowest))
               ; (* Eliminate a comparison filter. *)
                 choose (for_all tf Path.(all >>? test >>? is_run_time)) id
               ; lift
                   (O.seq_many
                      [ push_all_unparameterized_filters
                      ; O.for_all S.row_store
                          Path.(all >>? is_run_time >>? is_relation)
                      ; push_all_unparameterized_filters
                      ; fix project
                      ; Simplify_tactic.simplify ]) ]
             |> lower (min Type_cost.(cost ~kind:`Avg read))) ])

  let try_partition tf =
    Branching.(
      seq_many [choose (traced F.partition) id; lift tf]
      |> lower (min Type_cost.(cost ~kind:`Avg read)))

  let try_ tf rest =
    Branching.(
      seq (choose (lift tf) id) (lift rest)
      |> lower (min Type_cost.(cost ~kind:`Avg read)))

  let opt =
    let open Infix in
    seq_many
      [ (* Eliminate groupby operators. *)
        fix
          (at_ Groupby_tactics.elim_groupby
             (Path.all >>? is_groupby >>| shallowest))
      ; (* Hoist parameterized filters as far up as possible. *)
        fix
          (at_ F.hoist_filter
             (Path.all >>? is_param_filter >>| deepest >>= parent))
        (* Eliminate unparameterized join nests. *)
      ; at_ Join_opt.transform
          (Path.all >>? is_join >>? not has_free >>| shallowest)
      ; push_all_unparameterized_filters
      ; project
      ; at_ Join_elim_tactics.elim_join_nest
          (Path.all >>? is_join >>| shallowest)
      ; try_
          (first F.elim_disjunct (Path.all >>? is_filter))
          (seq_many
             [ (* Push constant filters *)
               fix
                 (at_ F.push_filter
                    Castor.Path.(all >>? is_const_filter >>| shallowest))
             ; (* Push orderby operators into compile time position if possible. *)
               fix
                 (at_ push_orderby
                    Path.(all >>? is_orderby >>? is_run_time >>| shallowest))
             ; (* Eliminate comparison filters. *)
               elim_param_filter F.elim_cmp_filter is_param_cmp_filter
             ; (* Eliminate the deepest equality filter. *)
               elim_param_filter
                 (Branching.lift F.elim_eq_filter)
                 is_param_filter
             ; push_all_unparameterized_filters
             ; (* Eliminate all unparameterized relations. *)
               fix
                 (seq_many
                    [ at_ S.row_store
                        Path.(
                          all >>? is_run_time >>? not has_params
                          >>? not is_serializable
                          >>? not (contains is_collection)
                          >>| shallowest)
                    ; push_all_unparameterized_filters ])
             ; push_all_unparameterized_filters
             ; (* Push selections above collections. *)
               fix
                 (for_all Select_tactics.push_select
                    Path.(
                      all >>? is_select >>? is_run_time >>? above is_collection))
             ; (* Push orderby operators into compile time position if possible. *)
               fix
                 (at_ push_orderby
                    Path.(all >>? is_orderby >>? is_run_time >>| shallowest))
               (* Last-ditch tactic to eliminate orderby. *)
             ; for_all S.row_store Path.(all >>? is_orderby >>? is_run_time)
             ; (* Try throwing away structure if it reduces overall cost. *)
               Branching.(
                 seq_many
                   [ choose id
                       (seq_many
                          [ for_all (lift S.row_store)
                              Path.(all >>? is_run_time >>? not has_params)
                          ; lift push_all_unparameterized_filters ])
                   ; filter is_serializable ]
                 |> lower (min Type_cost.(cost ~kind:`Avg read)))
               (* Cleanup*)
             ; fix (for_all Dedup_tactics.push_dedup Path.(all >>? is_dedup))
             ; fix project
             ; push_all_unparameterized_filters
             ; Simplify_tactic.simplify ]) ]

  let opt_toplevel = seq_many [try_partition opt; apply_to_subqueries opt]

  let is_serializable r =
    let r = R.resolve ~params r in
    annotate_free r ;
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
end

let optimize (module C : Config.S) r =
  (* Annotate query with free variables. *)
  let module M = Abslayout_db.Make (C) in
  annotate_free r ;
  let module T = Make (C) () in
  let module O = Ops.Make (C) in
  (* Optimize outer query. *)
  O.apply T.opt_toplevel Path.root r
