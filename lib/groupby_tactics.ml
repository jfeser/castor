open Core
open Castor
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Ops.Config.S

    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module Ops = Ops.Make (C)
  open Ops
  module M = Abslayout_db.Make (C)
  module P = Project.Make (C)

  let src = Logs.Src.create "groupby-tactics"

  (** Remove all references to names in params while ensuring that the resulting
     relation overapproximates the original. *)
  let over_approx params r =
    let visitor =
      object (self)
        inherit [_] map as super

        method! visit_Filter () (p, r) =
          if Set.is_empty (Set.inter (Pred.names p) params) then
            super#visit_Filter () (p, r)
          else (self#visit_t () r).node

        method! visit_Select () (ps, r) =
          match select_kind ps with
          | `Agg -> Select (ps, r)
          | `Scalar -> Select (ps, self#visit_t () r)

        method! visit_GroupBy () ps ks r = GroupBy (ps, ks, r)
      end
    in
    let r = visitor#visit_t () r in
    let remains = Set.inter (free r) params in
    if Set.is_empty remains then Ok r
    else
      Or_error.error "Failed to remove all parameters." remains
        [%sexp_of: Set.M(Name).t]

  let _base_relation_approx key r =
    let exception Failed of Error.t in
    let value_exn err = function Some v -> v | None -> raise (Failed err) in
    (* Otherwise, if all grouping keys are from named relations, select all
       possible grouping keys. *)
    let defs = Meta.(find_exn r defs) in
    let rels = Hashtbl.create (module Abslayout) in
    let alias_map = aliases r in
    try
      (* Find the definition of each key. *)
      let key_defs =
        List.map key ~f:(fun n ->
            List.find_map defs ~f:(fun (n', p) ->
                Option.bind n' ~f:(fun n' ->
                    if Name.O.(n = n') then Some p else None ) )
            |> value_exn
                 (Error.create "No definition found for key." n
                    [%sexp_of: Name.t]) )
      in
      (* Collect all the names in each definition. If they all come from base
         relations, then we can enumerate the keys. *)
      List.iter key_defs ~f:(fun p ->
          Set.iter (Pred.names p) ~f:(fun n ->
              let r =
                Name.rel n
                |> value_exn
                     (Error.create "Name does not come from base relation." n
                        [%sexp_of: Name.t])
                |> Map.find alias_map
                |> value_exn
                     (Error.create "Relation not found in alias table." n
                        [%sexp_of: Name.t])
              in
              Hashtbl.add_multi rels ~key:r ~data:(Name n) ) ) ;
      let key_rel =
        Hashtbl.to_alist rels
        |> List.map ~f:(fun (r, ns) -> dedup (select ns r))
        |> List.fold_left1_exn ~f:(join (Bool true))
      in
      Ok (select key_defs key_rel)
    with Failed err -> Error err

  let elim_groupby r =
    annotate_free r ;
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name Global.fresh "k%d" in
        let key_preds = List.map key ~f:(fun n -> Name n) in
        let filter_pred =
          List.map key ~f:(fun n ->
              Binop (Eq, Name n, Name (Name.copy n ~scope:(Some key_name))) )
          |> List.fold_left1_exn ~f:(fun acc p -> Binop (And, acc, p))
        in
        let keys = P.project ~params:(free r) (dedup (select key_preds r)) in
        (* Try to remove any remaining parameters from the keys relation. *)
        match over_approx C.params keys with
        | Ok keys ->
            Some (list keys key_name (select ps (filter filter_pred r)))
        | Error err ->
            Logs.info ~src (fun m -> m "elim-groupby: %a" Error.pp err) ;
            None )
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby = of_func elim_groupby ~name:"elim-groupby"
end

module Test = struct
  module C = struct
    let conn = Db.create "postgresql:///tpch_1k"

    let verbose = false

    let validate = false

    let params =
      let open Type.PrimType in
      Set.of_list
        (module Name)
        [ Name.create ~type_:string_t "param1"
        ; Name.create ~type_:string_t "param2"
        ; Name.create ~type_:string_t "param3" ]

    let param_ctx = Map.empty (module Name)

    let fresh = Fresh.create ()
  end

  module T = Make (C)
  open C
  open T
  open Ops

  let with_logs f =
    Logs.(set_reporter (format_reporter ())) ;
    Logs.Src.set_level src (Some Debug) ;
    let ret = f () in
    Logs.Src.set_level src (Some Error) ;
    Logs.(set_reporter nop_reporter) ;
    ret

  let%expect_test "" =
    let r =
      M.load_string ~params
        {|
groupby([o_year,
         (sum((if (nation_name = param1) then volume else 0.0)) /
         sum(volume)) as mkt_share],
  [o_year],
  select([to_year(o_orderdate) as o_year,
          (l_extendedprice * (1 - l_discount)) as volume,
          n2_name as nation_name],
    join((p_partkey = l_partkey),
      join((s_suppkey = l_suppkey),
        join((l_orderkey = o_orderkey),
          join((o_custkey = c_custkey),
            join((c_nationkey = n1_nationkey),
              join((n1_regionkey = r_regionkey),
                select([n_regionkey as n1_regionkey, n_nationkey as n1_nationkey],
                  nation),
                filter((r_name = param2), region)),
              customer),
            filter(((o_orderdate >= date("1995-01-01")) &&
                   (o_orderdate <= date("1996-12-31"))),
              orders)),
          lineitem),
        join((s_nationkey = n2_nationkey),
          select([n_nationkey as n2_nationkey, n_name as n2_name],
            nation),
          supplier)),
      filter((p_type = param3), part))))
|}
    in
    with_logs (fun () ->
        apply elim_groupby r
        |> Option.iter ~f:(Format.printf "%a@." Abslayout.pp) ) ;
    [%expect
      {|
      alist(dedup(
              select([o_year],
                select([to_year(o_orderdate) as o_year],
                  filter(((o_orderdate >= date("1995-01-01")) &&
                         (o_orderdate <= date("1996-12-31"))),
                    orders)))) as k0,
        select([o_year,
                (sum((if (nation_name = param1) then volume else 0.0)) /
                sum(volume)) as mkt_share],
          filter((o_year = k0.o_year),
            select([to_year(o_orderdate) as o_year,
                    (l_extendedprice * (1 - l_discount)) as volume,
                    n2_name as nation_name],
              join((p_partkey = l_partkey),
                join((s_suppkey = l_suppkey),
                  join((l_orderkey = o_orderkey),
                    join((o_custkey = c_custkey),
                      join((c_nationkey = n1_nationkey),
                        join((n1_regionkey = r_regionkey),
                          select([n_regionkey as n1_regionkey,
                                  n_nationkey as n1_nationkey],
                            nation),
                          filter((r_name = param2), region)),
                        customer),
                      filter(((o_orderdate >= date("1995-01-01")) &&
                             (o_orderdate <= date("1996-12-31"))),
                        orders)),
                    lineitem),
                  join((s_nationkey = n2_nationkey),
                    select([n_nationkey as n2_nationkey, n_name as n2_name],
                      nation),
                    supplier)),
                filter((p_type = param3), part)))))) |}]
end
