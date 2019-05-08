open Core
open Abslayout
open Collections

module Config = struct
  module type S = sig
    include Abslayout_db.Config.S
  end
end

module Make (C : Config.S) = struct
  module M = Abslayout_db.Make (C)

  let test =
    let src = Logs.Src.create ~doc:"Source for testing project." "project-test" in
    Logs.Src.set_level src None ; src

  let project_defs refcnt ps =
    List.filter ps ~f:(fun p ->
        match Pred.to_name p with
        | None ->
            (* Filter out definitions that have no name *)
            false
        | Some n -> (
          (* Filter out definitions that are never referenced. *)
          match Map.(find refcnt n) with
          | Some c -> c > 0
          | None ->
              (* Be conservative if refcount is missing. *)
              true ) )

  let all_unref r =
    let refcnt = Meta.find_exn r M.refcnt in
    let schema = schema_exn r in
    List.for_all (Schema.unscoped schema) ~f:(fun n ->
        match Map.(find refcnt n) with Some c -> c = 0 | None -> false )

  let pp_with_refcount, _ =
    mk_pp
      ~pp_meta:(fun fmt meta ->
        let open Format in
        match Univ_map.find meta M.refcnt with
        | Some r ->
            fprintf fmt "@[<hv 2>{" ;
            Map.iteri r ~f:(fun ~key:n ~data:c ->
                if c > 0 then fprintf fmt "%a=%d,@ " Name.pp n c ) ;
            fprintf fmt "}@]"
        | None -> () )
      ()

  type count = AtLeastOne | Exact [@@deriving sexp]

  let count = Univ_map.Key.create ~name:"count" [%sexp_of: count]

  let annotate_count r =
    let count_matters =
      List.exists ~f:(function Count | Sum _ | Avg _ -> true | _ -> false)
    in
    let visitor =
      object (self)
        inherit [_] map as super

        method! visit_t c r =
          let c =
            match r.node with
            | Dedup _ -> AtLeastOne
            | Select (s, _) | GroupBy (s, _, _) ->
                if count_matters s then Exact else c
            | _ -> c
          in
          Meta.set (super#visit_t c r) count c

        method! visit_AHashIdx c (rk, rv, m) =
          AHashIdx
            (self#visit_t AtLeastOne rk, self#visit_t c rv, self#visit_hash_idx c m)

        method! visit_AOrderedIdx c (rk, rv, m) =
          AOrderedIdx
            ( self#visit_t AtLeastOne rk
            , self#visit_t c rv
            , self#visit_ordered_idx c m )
      end
    in
    visitor#visit_t Exact r

  let project_visitor =
    object (self : 'a)
      inherit [_] map as super

      method! visit_t () r =
        let refcnt = Meta.find_exn r M.refcnt in
        let count = Meta.find_exn r count in
        if all_unref r && count = AtLeastOne then scalar Null
        else
          match r.node with
          | Select (ps, r) -> select (project_defs refcnt ps) (self#visit_t () r)
          | Dedup r -> dedup (self#visit_t () r)
          | GroupBy (ps, ns, r) ->
              group_by (project_defs refcnt ps) ns (self#visit_t () r)
          | AScalar p -> (
            match project_defs refcnt [p] with
            | [] -> scalar Null
            | [p] -> scalar p
            | _ -> assert false )
          | ATuple ([], _) -> empty
          | ATuple ([r], _) -> self#visit_t () r
          | ATuple (rs, Concat) ->
              let rs = List.map rs ~f:(self#visit_t ()) in
              let rs = List.filter rs ~f:(fun r -> r.node <> AEmpty) in
              tuple rs Concat
          | ATuple (rs, Cross) ->
              let rs =
                (* Remove unreferenced parts of the tuple. *)
                List.filter rs ~f:(fun r ->
                    let is_unref = all_unref r in
                    let is_scalar =
                      match r.node with AScalar _ -> true | _ -> false
                    in
                    let should_remove =
                      match count with
                      (* If the count matters, then we can only remove
                           unreferenced scalars. *)
                      | Exact -> is_unref && is_scalar
                      (* Otherwise we can remove anything unreferenced. *)
                      | AtLeastOne -> is_unref
                    in
                    if should_remove then
                      Logs.debug ~src:test (fun m ->
                          m "Removing tuple element %a." pp_with_refcount r ) ;
                    not should_remove )
              in
              let rs = if List.length rs = 0 then [scalar Null] else rs in
              tuple rs Cross
          | Join {r1; r2; pred} -> (
            match count with
            | Exact -> join pred (self#visit_t () r1) (self#visit_t () r2)
            (* If one side of a join is unused then the join can be dropped. *)
            | AtLeastOne ->
                if all_unref r1 then self#visit_t () r2
                else if all_unref r2 then self#visit_t () r1
                else join pred (self#visit_t () r1) (self#visit_t () r2) )
          | DepJoin {d_lhs; d_rhs; d_alias} -> (
            match count with
            | Exact ->
                dep_join (self#visit_t () d_lhs) d_alias (self#visit_t () d_rhs)
            (* If one side of a join is unused then the join can be dropped. *)
            | AtLeastOne ->
                if all_unref d_lhs then self#visit_t () d_rhs
                else if all_unref d_rhs then scalar Null
                else
                  dep_join (self#visit_t () d_lhs) d_alias (self#visit_t () d_rhs) )
          | _ -> super#visit_t () r
    end

  let project_once r =
    Logs.debug ~src:test (fun m -> m "pre %a@." pp_with_refcount r) ;
    let r = annotate_count r in
    let r = project_visitor#visit_t () r in
    Logs.debug ~src:test (fun m -> m "post %a@." pp r) ;
    r

  let project ?(params = Set.empty (module Name)) r =
    let rec loop r =
      let r' = M.resolve r ~params |> project_once in
      if Abslayout.O.(r = r') then r' else loop r'
    in
    loop r
end

module Test = struct
  module T = Make (struct
    let conn = Db.create "postgresql:///tpch_1k"
  end)

  open T

  let with_logs f =
    Logs.(set_reporter (format_reporter ())) ;
    Logs.Src.set_level test (Some Debug) ;
    let ret = f () in
    Logs.Src.set_level test (Some Error) ;
    Logs.(set_reporter nop_reporter) ;
    ret
end
