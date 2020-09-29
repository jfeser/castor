open! Core
open Castor
open Collections
open Castor_opt
open Abslayout_load
module A = Abslayout
open Match

module type CONFIG = sig
  val conn : Db.t

  val params : Set.M(Name).t

  val cost_conn : Db.t
end

module Xforms (C : CONFIG) = struct
  module O = Ops.Make (C)
  module Simplify = Simplify_tactic.Make (C)
  module Filter = Filter_tactics.Make (C)
  module Groupby = Groupby_tactics.Make (C)
  module Orderby = Orderby_tactics.Make (C)
  module Simple = Simple_tactics.Make (C)
  module Join_elim = Join_elim_tactics.Make (C)
  module Select = Select_tactics.Make (C)
end

let config conn params =
  ( module struct
    let conn = conn

    let cost_conn = conn

    let params = params
  end : CONFIG )

let main ~name ~params ~ch =
  let conn = Db.create (Sys.getenv_exn "CASTOR_DB") in
  let params =
    List.map params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let query = load_string_exn ~params conn @@ In_channel.input_all ch in

  let (module C) = config conn params in
  let open Xforms (C) in
  let open O in
  let open Simplify in
  let open Filter in
  let open Groupby in
  let open Orderby in
  let open Select in
  let open Simple in
  let open Join_elim in
  (* Recursively optimize subqueries. *)
  let apply_to_subqueries tf =
    let subquery_visitor tf =
      object (self : 'a)
        inherit [_] V.map

        method visit_subquery r =
          Option.value_exn ~message:"Transforming subquery failed."
            (apply tf Path.root r)

        method! visit_Exists () r = Exists (self#visit_subquery r)

        method! visit_First () r = First (self#visit_subquery r)
      end
    in

    let f r = Some ((subquery_visitor tf)#visit_t () r) in
    of_func f ~name:"apply-to-subqueries"
  in

  let apply_to_filter_subquery tf =
    let open Option.Let_syntax in
    let subquery_visitor tf =
      object (self : 'a)
        inherit [_] V.map

        method visit_subquery r =
          let module C = struct
            let conn = conn

            let cost_conn = conn

            let params = Set.union params (Free.free r)
          end in
          Option.value_exn ~message:"Transforming subquery failed."
            (apply (tf (module C : CONFIG)) Path.root r)

        method! visit_Exists () r = Exists (self#visit_subquery r)

        method! visit_First () r = First (self#visit_subquery r)
      end
    in

    let f r =
      let%bind p, r' = to_filter r in
      return @@ A.filter ((subquery_visitor tf)#visit_pred () p) r'
    in
    of_func f ~name:"apply-to-filter-subquery"
  in

  let xform_1 =
    seq_many
      [
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ push_orderby Path.(all >>? is_orderby >>| shallowest);
        Branching.at_ elim_cmp_filter Path.(all >>? is_filter >>| deepest)
        |> Branching.lower Seq.hd;
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        project;
        simplify;
        at_ push_select Path.(all >>? is_select >>| shallowest);
        at_ row_store Path.(all >>? is_filter >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_2 =
    seq_many
      [
        id;
        at_ hoist_filter
          Path.(all >>? is_join >>? has_child is_filter >>| deepest);
        at_ hoist_filter
          Path.(all >>? is_join >>? has_child is_filter >>| deepest);
        at_ hoist_filter
          Path.(all >>? is_join >>? has_child is_filter >>| deepest);
        at_ hoist_filter
          Path.(all >>? is_join >>? has_child is_filter >>| deepest);
        at_ hoist_filter
          Path.(all >>? is_join >>? has_child is_filter >>| deepest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ hoist_filter Path.(all >>? is_orderby >>| shallowest);
        at_ elim_eq_filter Path.(all >>? is_filter >>| shallowest);
        at_ push_orderby Path.(all >>? is_orderby >>| shallowest);
        simplify;
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        at_
          (split_out
             ( Path.(
                 all >>? is_relation
                 >>? matches (function
                       | Relation r -> String.(r.r_name = "supplier")
                       | _ -> false)
                 >>| deepest)
             >>= parent )
             "s1_suppkey")
          Path.(all >>? is_filter >>| shallowest);
        fix project;
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ hoist_filter Path.(all >>? is_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_
          ( precompute_filter "p_type"
          @@ List.map ~f:(sprintf "\"%s\"")
               [ "TIN"; "COPPER"; "NICKEL"; "BRASS"; "STEEL" ] )
          (Path.(all >>? is_param_filter >>| shallowest) >>= child' 0);
        at_ row_store (Path.(all >>? is_param_filter >>| deepest) >>= child' 0);
        at_ row_store
          (Path.(all >>? is_hash_idx >>| deepest) >>= child' 1 >>= child' 0);
        project;
        simplify;
      ]
  in

  let xform_3 =
    seq_many
      [
        fix
          (at_ hoist_filter
             (Path.(all >>? is_filter >>| shallowest) >>= parent));
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        fix
          (at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent));
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        Branching.at_
          (partition_eq "customer.c_mktsegment")
          Path.(all >>? is_collection >>| shallowest)
        |> Branching.lower Seq.hd;
        fix
          (at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent));
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ row_store
          Infix.(
            Path.(all >>? is_filter >>? not is_param_filter >>| shallowest));
        project;
        simplify;
      ]
  in

  let xform_4 =
    seq_many
      [
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ push_orderby Path.(all >>? is_orderby >>| shallowest);
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        Branching.at_ elim_cmp_filter Path.(all >>? is_filter >>| shallowest)
        |> Branching.lower Seq.hd;
        fix @@ at_ push_filter Path.(all >>? is_filter >>| shallowest);
        simplify;
        at_ push_select Path.(all >>? is_select >>| shallowest);
        at_ row_store Path.(all >>? is_filter >>| shallowest);
        project;
        simplify;
      ]
  in

  let swap_filter p = seq_many [ at_ hoist_filter p; at_ split_filter p ] in

  let xform_5 =
    seq_many
      [
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ elim_groupby_approx Path.(all >>? is_groupby >>| shallowest);
        Branching.at_
          (partition_eq "region.r_name")
          Path.(all >>? is_collection >>| shallowest)
        |> Branching.lower Seq.hd;
        swap_filter Path.(all >>? is_filter >>| shallowest);
        Branching.at_ elim_cmp_filter
          Path.(all >>? is_param_filter >>| shallowest)
        |> Branching.lower Seq.hd;
        simplify;
        at_ push_select Path.(all >>? is_select >>? is_run_time >>| deepest);
        at_ row_store Path.(all >>? is_filter >>? is_run_time >>| shallowest);
        project;
        simplify;
      ]
  in

  let push_no_param_filter =
    fix
    @@ at_ push_filter
         Infix.(Path.(all >>? is_filter >>? not is_param_filter >>| shallowest))
  in

  let xform_6 =
    seq_many
      [
        fix @@ at_ split_filter Path.(all >>? is_filter >>| deepest);
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        at_ push_filter Path.(all >>? is_filter >>| shallowest >>= child' 0);
        at_ hoist_filter Path.(all >>? is_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ hoist_filter Path.(all >>? is_filter >>| shallowest >>= child' 0);
        at_ split_filter Path.(all >>? is_filter >>| shallowest >>= child' 0);
        Branching.at_ elim_cmp_filter Path.(all >>? is_filter >>| deepest)
        |> Branching.lower Seq.hd;
        Branching.at_ elim_cmp_filter
          Path.(all >>? is_filter >>| shallowest >>= child' 0)
        |> Branching.lower Seq.hd;
        push_no_param_filter;
        at_ row_store Path.(all >>? is_filter >>| deepest);
        fix project;
        simplify;
      ]
  in

  let xform_7 =
    seq_many
      [
        at_
          (partition_domain "param0" "nation.n_name")
          Path.(all >>? is_orderby >>| shallowest);
        at_
          (partition_domain "param1" "nation.n_name")
          Path.(all >>? is_collection >>| shallowest);
        at_ row_store Path.(all >>? is_orderby >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_8 =
    seq_many
      [
        Branching.at_
          (partition_eq "region.r_name")
          Path.(all >>? is_orderby >>| shallowest)
        |> Branching.lower Seq.hd;
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ push_orderby Path.(all >>? is_orderby >>| shallowest);
        push_no_param_filter;
        at_ hoist_join_param_filter Path.(all >>? is_join >>| shallowest);
        at_ row_store Path.(all >>? is_join >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_9 =
    seq_many
      [
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ push_orderby Path.(all >>? is_orderby >>| shallowest);
        at_ hoist_filter_extend
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ hoist_filter
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        at_
          ( precompute_filter_bv
          @@ List.map ~f:(sprintf "\"%s\"")
               [
                 "black";
                 "blue";
                 "brown";
                 "green";
                 "grey";
                 "navy";
                 "orange";
                 "pink";
                 "purple";
                 "red";
                 "white";
                 "yellow";
               ] )
          Path.(all >>? is_param_filter >>| shallowest);
        at_ row_store
          (Path.(all >>? is_param_filter >>| shallowest) >>= child' 0);
        project;
        simplify;
      ]
  in

  let xform_10 =
    seq_many
      [
        at_ elim_groupby_flat Path.(all >>? is_groupby >>| shallowest);
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        at_ hoist_filter
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        at_ row_store
          (Path.(all >>? is_param_filter >>| shallowest) >>= child' 0);
        project;
        simplify;
      ]
  in

  let xform_11 =
    seq_many
      [
        at_
          (partition_domain "param1" "nation.n_name")
          Path.(all >>? is_filter >>| shallowest);
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ elim_subquery Path.(all >>? is_filter >>| shallowest);
        at_ row_store (Path.(all >>? is_list >>| shallowest) >>= child' 1);
        at_ hoist_param (Path.(all >>? is_depjoin >>| shallowest) >>= child' 0);
        at_ row_store
          (Path.(all >>? is_depjoin >>| shallowest) >>= child' 0 >>= child' 0);
        project;
        simplify;
      ]
  in

  let xform_12 =
    seq_many
      [
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        push_orderby;
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ hoist_filter_agg
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| deepest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| deepest);
        at_ split_filter Path.(all >>? is_param_filter >>| deepest);
        at_ split_filter Path.(all >>? is_param_filter >>| deepest);
        at_ hoist_filter (Path.(all >>? is_param_filter >>| deepest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| deepest);
        at_ hoist_filter (Path.(all >>? is_param_filter >>| deepest) >>= parent);
        at_ split_filter Path.(all >>? is_param_filter >>| deepest);
        Branching.at_ elim_cmp_filter Path.(all >>? is_param_filter >>| deepest)
        |> Branching.lower Seq.hd;
        simplify;
        at_ push_select Path.(all >>? is_select >>? is_run_time >>| shallowest);
        at_ row_store
          Infix.(
            Path.(all >>? is_filter >>? not is_param_filter >>| shallowest));
        project;
        simplify;
      ]
  in

  let xform_14 =
    seq_many
      [
        at_ hoist_filter
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        Branching.at_ elim_cmp_filter Path.(all >>? is_filter >>| shallowest)
        |> Branching.lower Seq.hd;
        simplify;
        push_select;
        at_ row_store Path.(all >>? is_filter >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_15 =
    seq_many
      [
        at_
          (partition_domain "param1" "lineitem.l_shipdate")
          Path.(all >>? is_orderby >>| shallowest);
        at_ row_store Path.(all >>? is_orderby >>| shallowest);
        at_ hoist_join_filter Path.(all >>? is_join >>| shallowest);
        at_ elim_subquery_join Path.(all >>? is_filter >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_16 =
    seq_many
      [
        fix
        @@ at_ hoist_filter
             (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ hoist_filter_extend
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        Branching.at_ elim_groupby_partial
          Path.(all >>? is_groupby >>| shallowest)
        |> Branching.lower (fun s -> Seq.nth s 2);
        at_ row_store Path.(all >>? is_groupby >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_17 =
    seq_many
      [
        at_ hoist_filter
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        at_ elim_eq_filter Path.(all >>? is_param_filter >>| shallowest);
        apply_to_subqueries
          (seq_many
             [
               at_
                 (partition_domain "p1_partkey" "part.p_partkey")
                 Path.(all >>? is_select >>| shallowest);
               at_ row_store
                 Path.(all >>? is_select >>? is_run_time >>| deepest);
             ]);
        at_ row_store Path.(all >>? is_filter >>| deepest);
        simplify;
        project;
        project;
      ]
  in

  let xform_18 =
    seq_many
      [
        fix
        @@ at_ hoist_filter (Path.(all >>? is_filter >>| shallowest) >>= parent);
        apply_to_subqueries
          (seq_many
             [
               split_filter;
               at_ hoist_filter
                 (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
               split_filter;
               at_
                 (partition_domain "o1_orderkey" "lineitem.l_orderkey")
                 (Path.(all >>? is_param_filter >>| shallowest) >>= child' 0);
               at_ row_store
                 (Path.(all >>? is_collection >>| shallowest) >>= child' 1);
               simplify;
             ]);
        project;
        at_ row_store Path.(all >>? is_orderby >>| shallowest);
        project;
        simplify;
      ]
  in

  let xform_19 =
    seq_many
      [
        at_ hoist_join_param_filter Path.(all >>? is_join >>| shallowest);
        at_ elim_disjunct Path.(all >>? is_filter >>| shallowest);
        fix
        @@ at_ split_filter_params
             ( Path.(all >>? above is_param_filter >>? is_join >>| deepest)
             >>= parent );
        fix
        @@ at_ row_store
             Infix.(
               Path.(
                 all >>? is_filter >>? not is_param_filter >>? is_run_time
                 >>| shallowest));
        project;
        try_ simplify;
      ]
  in

  let xform_20 =
    seq_many
      [
        partition_domain "param2" "nation.n_name";
        at_ hoist_filter Path.(all >>? is_join >>| shallowest);
        at_ hoist_filter Path.(all >>? is_orderby >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_ hoist_filter Path.(all >>? is_filter >>| shallowest);
        at_ split_filter Path.(all >>? is_filter >>| shallowest);
        at_
          ( apply_to_filter_subquery @@ fun (module C : CONFIG) ->
            seq_many
              [
                at_
                  ( apply_to_filter_subquery @@ fun _ ->
                    seq_many
                      [
                        Branching.at_
                          (partition_eq "part.p_partkey")
                          Path.(all >>? is_filter >>| shallowest)
                        |> Branching.lower Seq.hd;
                        swap_filter Path.(all >>? is_filter >>| shallowest);
                        first row_store Path.(all >>? is_run_time);
                      ] )
                  Path.(all >>? is_filter >>| shallowest);
                at_
                  ( apply_to_filter_subquery @@ fun (module C : CONFIG) ->
                    let open Filter_tactics.Make (C) in
                    let open Simplify_tactic.Make (C) in
                    let open Select_tactics.Make (C) in
                    seq_many
                      [
                        at_ elim_eq_filter Path.(all >>? is_filter >>| deepest);
                        at_ push_filter
                          Path.(all >>? is_param_filter >>| shallowest);
                        Branching.at_ elim_cmp_filter
                          Path.(all >>? is_param_filter >>| shallowest)
                        |> Branching.lower Seq.hd;
                        simplify;
                        at_ push_select Path.(all >>? is_select >>| shallowest);
                        simplify;
                        at_ push_select
                          Path.(all >>? is_select >>? is_run_time >>| deepest);
                        project;
                        first row_store Path.(all >>? is_run_time);
                        project;
                      ] )
                  (Path.(all >>? is_filter >>| shallowest) >>= child' 0);
                (let open Filter_tactics.Make (C) in
                at_ elim_eq_filter Path.(all >>? is_filter >>| deepest));
                at_ row_store
                  Path.(all >>? is_run_time >>? is_filter >>| deepest);
              ] )
          Path.(all >>? is_filter >>| shallowest);
        first row_store Path.(all >>? is_run_time);
        project;
      ]
  in
  let xform_21 =
    seq_many
      [
        partition_domain "param0" "nation.n_name";
        first row_store Path.(all >>? is_run_time);
        simplify;
        project;
      ]
  in
  let xform_22 =
    seq_many
      [
        elim_groupby;
        for_all push_select Path.(all >>? is_select);
        for_all cse_filter Path.(all >>? is_filter);
        for_all push_select Path.(all >>? is_select);
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        swap_filter Path.(all >>? is_filter >>| shallowest);
        swap_filter (Path.(all >>? is_filter >>| shallowest) >>= child' 0);
        at_
          ( apply_to_filter_subquery @@ fun (module C : CONFIG) ->
            let open Ops.Make (C) in
            let open Filter_tactics.Make (C) in
            seq_many
              [
                for_all cse_filter Path.(all >>? is_filter);
                first row_store Path.all;
                simplify;
                project;
              ] )
          (Path.(all >>? is_filter >>| shallowest) >>= child' 0);
        first row_store Path.(all >>? is_run_time);
        elim_subquery;
        simplify;
        project;
      ]
  in

  let xform =
    match name with
    | "1" -> xform_1
    | "2" -> xform_2
    | "3-no" -> xform_3
    | "4" -> xform_4
    | "5-no" -> xform_5
    | "6" -> xform_6
    | "7" -> xform_7
    | "8" -> xform_8
    | "9" -> xform_9
    | "10-no" -> xform_10
    | "11-no" -> xform_11
    | "12" -> xform_12
    | "14" -> xform_14
    | "15" -> xform_15
    | "16-no" -> xform_16
    | "17" -> xform_17
    | "18" -> xform_18
    | "19" -> xform_19
    | "20" -> xform_20
    | "21-no" -> xform_21
    | "22-no" -> xform_22
    | _ -> failwith "unknown query name"
  in
  let query' = apply xform Path.root query in
  Option.iter query' ~f:(A.pp Fmt.stdout)

let () =
  let open Command.Let_syntax in
  Command.basic ~summary:"Apply transformations to a query."
    [%map_open
      let () = Log.param
      and () = Ops.param
      and () = Db.param
      and () = Type_cost.param
      and () = Join_opt.param
      and () = Groupby_tactics.param
      and () = Type.param
      and () = Simplify_tactic.param
      and params =
        flag "param" ~aliases:[ "p" ]
          (listed Util.param_and_value)
          ~doc:"NAME:TYPE query parameters"
      and name = flag "name" (required string) ~doc:"query name"
      and ch =
        anon (maybe_with_default In_channel.stdin ("query" %: Util.channel))
      in
      fun () -> main ~name ~params ~ch]
  |> Command.run
