open! Core
open Castor
open Collections
open Castor_opt
open Abslayout_load
module A = Abslayout

let main ~name ~params ~ch =
  let conn = Db.create (Sys.getenv_exn "CASTOR_DB") in
  let params =
    List.map params ~f:(fun (n, t, _) -> Name.create ~type_:t n)
    |> Set.of_list (module Name)
  in
  let query = load_string_exn ~params conn @@ In_channel.input_all ch in

  let module Config = struct
    let conn = conn

    let cost_conn = conn

    let params = params

    let cost_timeout = None

    let random = Mcmc.Random_choice.create ()
  end in
  let module T = Transform.Make (Config) in
  let open Ops.Make (Config) in
  let open T.Filter_tactics in
  let open T.Groupby_tactics in
  let open T.Orderby_tactics in
  let open T.Simple_tactics in
  let open T.Join_elim_tactics in
  let xform_1 =
    seq_many
      [
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ push_orderby Path.(all >>? is_orderby >>| shallowest);
        Branching.at_ elim_cmp_filter Path.(all >>? is_filter >>| deepest)
        |> Branching.lower Seq.hd;
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        at_ push_filter Path.(all >>? is_filter >>| shallowest);
        T.project;
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
        at_ T.Simple_tactics.row_store
          Infix.(
            Path.(all >>? is_filter >>? not is_param_filter >>| shallowest));
        T.project;
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
        at_ row_store Path.(all >>? is_filter >>| shallowest);
        T.project;
        T.project;
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
        fix T.project;
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
        T.project;
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
        T.project;
      ]
  in

  let xform_10 =
    seq_many
      [
        at_ elim_groupby Path.(all >>? is_groupby >>| shallowest);
        at_ elim_join_nest Path.(all >>? is_join >>? is_run_time >>| shallowest);
        fix @@ at_ push_filter Path.(all >>? is_filter >>| shallowest);
        at_ hoist_filter Path.(all >>? is_param_filter >>| deepest);
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
      ]
  in

  let xform_12 =
    seq_many
      [
        at_ split_filter Path.(all >>? is_param_filter >>| shallowest);
        at_ hoist_filter Path.(all >>? is_param_filter >>| shallowest);
      ]
  in

  let xform_14 =
    seq_many
      [
        at_ hoist_filter
          (Path.(all >>? is_param_filter >>| shallowest) >>= parent);
        Branching.at_ elim_cmp_filter Path.(all >>? is_filter >>| shallowest)
        |> Branching.lower Seq.hd;
        T.Simplify_tactic.simplify;
        T.Select_tactics.push_select;
        at_ row_store Path.(all >>? is_filter >>| shallowest);
        T.project;
      ]
  in

  let xform_15 =
    seq_many
      [
        at_
          (partition_domain "param1" "lineitem.l_shipdate")
          Path.(all >>? is_orderby >>| shallowest);
        at_ row_store Path.(all >>? is_orderby >>| shallowest);
      ]
  in

  let xform_16 =
    seq_many
      [
        at_
          (partition_domain "param1" "lineitem.l_shipdate")
          Path.(all >>? is_orderby >>| shallowest);
        at_ row_store Path.(all >>? is_orderby >>| shallowest);
      ]
  in

  let xform =
    match name with
    | "1" -> xform_1
    | "2" -> xform_2
    | "3" -> xform_3
    | "4" -> xform_4
    | "5" -> xform_5
    | "6" -> xform_6
    | "7" -> xform_7
    | "8" -> xform_8
    | "9" -> xform_9
    | "10" -> xform_10
    | "11" -> xform_11
    | "12" -> xform_12
    | "14" -> xform_14
    | "15" -> xform_15
    | "16" -> xform_16
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
