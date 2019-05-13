open Core
open Castor
open Collections
open Abslayout

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Filter_tactics.Config.S

    include Simple_tactics.Config.S

    include Join_opt.Config.S

    include Abslayout_db.Config.S
  end
end

module Make (Config : Config.S) () = struct
  open Config
  module M = Abslayout_db.Make (Config)
  module O = Ops.Make (Config)
  open O
  module F = Filter_tactics.Make (Config)
  open F
  module S = Simple_tactics.Make (Config)
  open S
  module P = Project.Make (Config)
  open P

  let is_serializable r p = is_serializeable (Path.get_exn p r)

  let has_params r p =
    let r' = Path.get_exn p r in
    overlaps (free r') params

  let project r = Some (project_once r)

  let project = of_func project ~name:"project"

  let push_orderby r =
    let key_is_supported r key =
      let s = Set.of_list (module Name) (schema_exn r) in
      List.for_all key ~f:(fun (p, _) -> is_supported s p)
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
        if List.for_all key ~f:(fun (p, _) -> is_supported s p) then
          Some (select ps (order_by key r))
        else None
    | OrderBy {key; rel= {node= Filter (ps, r); _}} ->
        Some (filter ps (order_by key r))
    | OrderBy {key; rel= {node= AHashIdx (r1, r2, m); _}} ->
        Some (hash_idx r1 (scope_exn r1) (order_by key r2) m)
    | OrderBy {key; rel= {node= AList (r1, r2); _}} ->
        (* If we order a lists keys then the keys will be ordered in the
                   list. *)
        orderby_list key r1 r2
    | OrderBy {key; rel= {node= ATuple (rs, Cross); _}} ->
        orderby_cross_tuple key rs
    | _ -> None

  let push_orderby = of_func push_orderby ~name:"push-orderby"

  let resolve =
    of_func (fun r -> M.resolve ~params r |> Option.some) ~name:"resolve"

  module Join_opt = Join_opt.Make (Config)
  module Simplify_tactic = Simplify_tactic.Make (Config)
  module Select_tactics = Select_tactics.Make (Config)
  module Groupby_tactics = Groupby_tactics.Make (Config)
  module Join_elim_tactics = Join_elim_tactics.Make (Config)

  let push_all_unparameterized_filters =
    fix
      (seq_many
         [resolve; first push_filter Path.(all >>? is_run_time >>? is_filter)])

  let opt =
    let open Infix in
    seq_many
      [ (* Eliminate groupby operators. *)
        fix
          (at_ Groupby_tactics.elim_groupby
             (Path.all >>? is_groupby >>| shallowest))
      ; (* Hoist parameterized filters as far up as possible. *)
        fix
          (at_ hoist_filter
             (Path.all >>? is_param_filter >>| deepest >>= parent))
        (* Eliminate unparameterized join nests. *)
      ; at_ Join_opt.transform
          (Path.all >>? is_join >>? not has_params >>| shallowest)
      ; at_ Join_elim_tactics.elim_join_nest
          (Path.all >>? is_join >>| shallowest)
        (* Push constant filters *)
      ; seq resolve
          (fix
             (at_ push_filter
                Castor.Path.(all >>? is_const_filter >>| shallowest)))
      ; (* Push orderby operators into compile time position if possible. *)
        fix
          (at_ push_orderby
             Path.(all >>? is_orderby >>? is_run_time >>| shallowest))
      ; (* Eliminate the shallowest equality filter. *)
        at_ elim_eq_filter
          Path.(all >>? is_param_eq_filter >>? is_run_time >>| shallowest)
      ; (* Eliminate the shallowest comparison filter. *)
        at_ elim_cmp_filter
          Path.(all >>? is_param_cmp_filter >>? is_run_time >>| shallowest)
      ; push_all_unparameterized_filters
      ; (* Push selections above collections. *)
        for_all Select_tactics.push_select
          Path.(all >>? is_select >>? is_run_time >>? above is_collection)
      ; (* Eliminate all unparameterized relations. *)
        fix
          (seq_many
             [ (* NOTE: Needed for is_serializable check. *)
               resolve
             ; at_ row_store
                 Path.(
                   all >>? is_run_time >>? not has_params
                   >>? not is_serializable >>? not is_collection >>| shallowest)
             ; push_all_unparameterized_filters ])
      ; push_all_unparameterized_filters
      ; (* Cleanup*)
        fix (seq resolve project)
      ; Simplify_tactic.simplify ]

  let is_serializable r =
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
             not (overlaps (free (Path.get_exn p r)) params) )
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
  (* Recursively optimize subqueries. *)
  let visitor =
    object (self : 'a)
      inherit [_] map

      method visit_subquery r =
        let module C = struct
          include C

          let params = Set.union params Meta.(find_exn r free)
        end in
        let module T = Make (C) () in
        let module O = Ops.Make (C) in
        Option.value_exn (O.apply T.opt r)

      method! visit_Exists () r = Exists (self#visit_subquery r)

      method! visit_First () r = First (self#visit_subquery r)
    end
  in
  let r = visitor#visit_t () r in
  let module T = Make (C) () in
  let module O = Ops.Make (C) in
  (* Optimize outer query. *)
  O.apply T.opt r
