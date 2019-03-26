open Base
open Core
open Castor
open Collections
open Abslayout

module Config = struct
  module type S = sig
    val conn : Db.t

    val dbconn : Postgresql.connection

    val params : Set.M(Name).t

    val param_ctx : Value.t Map.M(Name).t

    val validate : bool

    val fresh : Fresh.t
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

  let is_serializable r p =
    M.annotate_schema r ;
    is_serializeable (Path.get_exn p r)

  let sql_ctx = Sql.create_ctx ~fresh ()

  let has_params r p =
    let r' = Path.get_exn p r in
    overlaps (names r') params

  let project r =
    M.annotate_schema r ;
    Some (project r)

  let project = of_func project ~name:"project"

  let push_orderby r =
    let open Option.Let_syntax in
    let orderby_cross_tuple key rs =
      List.iter rs ~f:M.annotate_schema ;
      match rs with
      | r :: rs ->
          let schema = Meta.(find_exn r schema) in
          let sschema = Set.of_list (module Name) schema in
          let skey =
            Set.of_list
              (module Name)
              (List.filter_map ~f:(fun (p, _) -> pred_to_name p) key)
          in
          if Set.is_subset skey ~of_:sschema then
            Some (tuple (order_by key r :: rs) Cross)
          else None
      | _ -> None
    in
    let orderby_list key r1 r2 =
      M.annotate_schema r1 ;
      M.annotate_schema r2 ;
      annotate_eq r1 ;
      annotate_eq r2 ;
      let schema1 = Meta.(find_exn r1 schema) in
      let open Core in
      let eqs = Meta.(find_exn r2 eq) in
      let names =
        List.concat_map eqs ~f:(fun (n, n') -> [n; n'])
        @ List.filter_map ~f:(fun (p, _) -> pred_to_name p) key
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
          Union_find.union s s' ) ;
      let exception No_key in
      try
        let new_key =
          List.map key ~f:(fun (p, o) ->
              let p' =
                match pred_to_name p with
                | Some n -> (
                    let s = Hashtbl.find_exn eq_map n in
                    (* Find an equivalent name in schema 1. *)
                    let n' =
                      List.find schema1 ~f:(fun n' ->
                          let s' = Hashtbl.find_exn eq_map n' in
                          Union_find.same_class s s' )
                    in
                    match n' with Some n' -> Name n' | None -> raise No_key )
                | None -> raise No_key
              in
              (p', o) )
        in
        Some (list (order_by new_key r1) r2)
      with No_key -> None
    in
    let same_orders r1 r2 =
      M.annotate_schema r1 ;
      M.annotate_schema r2 ;
      annotate_eq r1 ;
      annotate_orders r1 ;
      annotate_eq r2 ;
      annotate_orders r2 ;
      [%compare.equal: (pred * order) list]
        Meta.(find_exn r1 order)
        Meta.(find_exn r2 order)
    in
    let%bind r' =
      match r.node with
      | OrderBy {key; rel= {node= Select (ps, r); _}} ->
          Some (select ps (order_by key r))
      | OrderBy {key; rel= {node= Filter (ps, r); _}} ->
          Some (filter ps (order_by key r))
      | OrderBy {key; rel= {node= AHashIdx (r1, r2, m); _}} ->
          Some (hash_idx' r1 (order_by key r2) m)
      | OrderBy {key; rel= {node= AList (r1, r2); _}} ->
          (* If we order a lists keys then the keys will be ordered in the
                   list. *)
          orderby_list key r1 r2
      | OrderBy {key; rel= {node= ATuple (rs, Cross); _}} ->
          orderby_cross_tuple key rs
      | _ -> None
    in
    if same_orders r r' then Some r' else None

  let push_orderby = of_func push_orderby ~name:"push-orderby"

  let elim_groupby r =
    M.annotate_schema r ;
    M.annotate_defs r ;
    annotate_free r ;
    match r.node with
    | GroupBy (ps, key, r) -> (
        let key_name = Fresh.name fresh "k%d" in
        let key_preds = List.map key ~f:(fun n -> Name n) in
        let filter_pred =
          List.map key ~f:(fun n ->
              Binop (Eq, Name n, Name (Name.copy n ~relation:(Some key_name)))
          )
          |> List.fold_left1_exn ~f:(fun acc p -> Binop (And, acc, p))
        in
        if Set.is_empty (free r) then
          (* Use precise version. *)
          Some
            (list
               (as_ key_name (dedup (select key_preds r)))
               (select ps (filter filter_pred r)))
        else
          let exception Failed of Error.t in
          let fail err = raise (Failed err) in
          (* Otherwise, if all grouping keys are from named relations,
             select all possible grouping keys. *)
          let defs = Meta.(find_exn r defs) in
          let rels = Hashtbl.create (module Abslayout) in
          let alias_map = aliases r in
          try
            (* Find the definition of each key and collect all the names in that
               definition. If they all come from base relations, then we can
               enumerate the keys. *)
            List.iter key ~f:(fun n ->
                let p =
                  List.find_map defs ~f:(fun (n', p) ->
                      Option.bind n' ~f:(fun n' ->
                          if Name.O.(n = n') then Some p else None ) )
                in
                let p =
                  match p with
                  | Some p -> p
                  | None ->
                      raise
                        (Failed
                           (Error.create "No definition found for key." n
                              [%sexp_of: Name.t]))
                in
                Set.iter (Pred.names p) ~f:(fun n ->
                    let r_name =
                      match Name.rel n with
                      | Some r -> r
                      | None ->
                          fail
                            (Error.create
                               "Name does not come from base relation." n
                               [%sexp_of: Name.t])
                    in
                    (* Look up relation in alias table. *)
                    let r =
                      match Map.find alias_map r_name with
                      | Some r -> r
                      | None ->
                          fail
                            (Error.create "Unknown relation." n
                               [%sexp_of: Name.t])
                    in
                    Hashtbl.add_multi rels ~key:r ~data:(Name n) ) ) ;
            let key_rel =
              Hashtbl.to_alist rels
              |> List.map ~f:(fun (r, ns) -> dedup (select ns r))
              |> List.fold_left1_exn ~f:(join (Bool true))
            in
            Some
              (list (as_ key_name key_rel) (select ps (filter filter_pred r)))
          with Failed err ->
            Logs.info (fun m -> m "%a" Error.pp err) ;
            None )
    (* Otherwise, if some keys are computed, fail. *)
    | _ -> None

  let elim_groupby = of_func elim_groupby ~name:"elim-groupby"

  module Join_opt = Join_opt.Make (Config)
  module Simplify_tactic = Simplify_tactic.Make (Config)
  module Select_tactics = Select_tactics.Make (Config)

  let opt =
    let open Infix in
    seq_many
      [ (* Eliminate groupby operators. *)
        fix (at_ elim_groupby (Path.all >>? is_groupby >>| shallowest))
      ; (* Hoist parameterized filters as far up as possible. *)
        fix
          (at_ hoist_filter
             (Path.all >>? is_param_filter >>| deepest >>= Path.parent))
      ; at_ Join_opt.transform (Path.all >>? is_join >>| shallowest)
      ; (* Push orderby operators into compile time position if possible. *)
        fix
          (at_ push_orderby
             Path.(all >>? is_orderby >>? is_run_time >>| shallowest))
      ; (* Eliminate the shallowest equality filter. *)
        at_ elim_eq_filter
          Path.(all >>? is_param_filter >>? is_run_time >>| shallowest)
      ; (* Eliminate the shallowest comparison filter. *)
        at_ elim_cmp_filter
          Path.(all >>? is_param_filter >>? is_run_time >>| shallowest)
      ; (* Push all unparameterized filters. *)
        fix
          (first push_filter
             Path.(all >>? is_run_time >>? is_filter >>? not is_param_filter))
      ; (* Push selections above collections. *)
        fix
          (at_ Select_tactics.push_select
             (Path.all >>? is_select >>? above is_collection >>| deepest))
      ; (* Eliminate all unparameterized relations. *)
        fix
          (seq_many
             [ at_ row_store
                 Path.(
                   all >>? is_run_time >>? not has_params
                   >>? not is_serializable >>| shallowest)
             ; project ])
        (* Cleanup*)
      ; fix project
      ; Simplify_tactic.simplify ]

  let is_serializable r =
    M.annotate_schema r ;
    annotate_free r ;
    let bad_runtime_op =
      Path.(
        all >>? is_run_time
        >>? Infix.(is_join || is_groupby || is_orderby || is_dedup || is_scan))
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
