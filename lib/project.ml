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
    Logs.Src.set_level src (Some Debug) ;
    src

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

  let all_unref refcnt schema =
    List.for_all schema ~f:(fun n ->
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

  let project_visitor =
    object (self : 'a)
      inherit [_] map as super

      method! visit_t count r =
        let open Option.Let_syntax in
        let r' =
          let%map refcnt = Univ_map.(find !(r.meta) M.refcnt) in
          let schema = schema_exn r in
          if (not count) && all_unref refcnt schema then empty
          else
            match r.node with
            | AList ({node= AEmpty; _}, _)
             |AList (_, {node= AEmpty; _})
             |AHashIdx ({node= AEmpty; _}, _, _)
             |AHashIdx (_, {node= AEmpty; _}, _)
             |AOrderedIdx ({node= AEmpty; _}, _, _)
             |AOrderedIdx (_, {node= AEmpty; _}, _)
             |Select (_, {node= AEmpty; _})
             |Filter (_, {node= AEmpty; _})
             |Dedup {node= AEmpty; _}
             |GroupBy (_, _, {node= AEmpty; _})
             |OrderBy {rel= {node= AEmpty; _}; _}
             |Join {r1= {node= AEmpty; _}; _}
             |Join {r2= {node= AEmpty; _}; _}
             |DepJoin {d_lhs= {node= AEmpty; _}; _}
             |DepJoin {d_rhs= {node= AEmpty; _}; _} ->
                empty
            | Select (ps, r) ->
                let count =
                  count || List.exists ps ~f:(function Count -> true | _ -> false)
                in
                select (project_defs refcnt ps) (self#visit_t count r)
            | Dedup r -> dedup (self#visit_t false r)
            | GroupBy (ps, ns, r) ->
                let count =
                  count || List.exists ps ~f:(function Count -> true | _ -> false)
                in
                group_by (project_defs refcnt ps) ns (self#visit_t count r)
            | AScalar p -> (
              match project_defs refcnt [p] with
              | [] -> if count then scalar Null else empty
              | [p] -> scalar p
              | _ -> assert false )
            | ATuple ([], _) -> empty
            | ATuple ([r], _) -> self#visit_t count r
            | ATuple (rs, Concat) ->
                let rs = List.map rs ~f:(self#visit_t count) in
                let rs = List.filter rs ~f:(fun r -> r.node <> AEmpty) in
                tuple rs Concat
            | ATuple (rs, Cross) ->
                if
                  List.exists rs ~f:(fun r ->
                      match r.node with AEmpty -> true | _ -> false )
                then empty
                else
                  let rs =
                    (* Remove unreferenced parts of the tuple. *)
                    List.filter rs ~f:(fun r ->
                        let is_unref =
                          all_unref
                            Univ_map.(find_exn !(r.meta) M.refcnt)
                            (schema_exn r)
                        in
                        let is_scalar =
                          match r.node with AScalar _ -> true | _ -> false
                        in
                        (* If the count matters, then we can only remove
                           unreferenced scalars. *)
                        let should_remove =
                          (count && is_unref && is_scalar)
                          || (* Otherwise we can remove anything unreferenced. *)
                             ((not count) && is_unref)
                        in
                        if should_remove then
                          Logs.debug ~src:test (fun m ->
                              m "Removing tuple element %a." pp_with_refcount r ) ;
                        not should_remove )
                  in
                  let rs =
                    (* We care about the count here (or at least the difference
                       between 1 record and none). *)
                    List.map rs ~f:(self#visit_t true)
                  in
                  let rs =
                    if count && List.length rs = 0 then [scalar Null] else rs
                  in
                  tuple rs Cross
            | Join {r1; r2; pred} ->
                (* If one side of a join is unused then the join can be dropped. *)
                let r1_unref = all_unref refcnt (schema_exn r1) in
                let r2_unref = all_unref refcnt (schema_exn r2) in
                let r1 = self#visit_t count r1 in
                let r2 = self#visit_t count r2 in
                if count then join pred r1 r2
                else if r1_unref then r2
                else if r2_unref then r1
                else join pred r1 r2
            | DepJoin {d_lhs; d_rhs; d_alias} ->
                (* If one side of a join is unused then the join can be dropped. *)
                let l_unref = all_unref refcnt (schema_exn d_lhs) in
                let r_unref = all_unref refcnt (schema_exn d_rhs) in
                let d_lhs = self#visit_t count d_lhs in
                let d_rhs = self#visit_t count d_rhs in
                if count then dep_join d_lhs d_alias d_rhs
                else if l_unref then d_rhs
                else if r_unref then empty
                else dep_join d_lhs d_alias d_rhs
            | AHashIdx (rk, rv, m) ->
                hash_idx (self#visit_t false rk) (scope_exn rk)
                  (self#visit_t count rv) m
            | AOrderedIdx (rk, rv, m) ->
                ordered_idx (self#visit_t false rk) (scope_exn rk)
                  (self#visit_t count rv) m
            | _ -> super#visit_t count r
        in
        match r' with Some r' -> r' | None -> r
    end

  let project_once r =
    Logs.debug ~src:test (fun m -> m "pre %a@." pp_with_refcount r) ;
    let r' = project_visitor#visit_t true r in
    Logs.debug ~src:test (fun m -> m "post %a@." pp r') ;
    r'

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
