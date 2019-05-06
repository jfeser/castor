open Core
open Abslayout
open Collections
open Name

module Config = struct
  module type S = sig end
end

module Make (C : Config.S) = struct
  let meta_ref = Univ_map.Key.create ~name:"meta-ref" [%sexp_of: Univ_map.t ref]

  let mut_refcnt =
    Univ_map.Key.create ~name:"mut-refcnt" [%sexp_of: int ref Map.M(Name).t]

  let refcnt = Univ_map.Key.create ~name:"refcnt" [%sexp_of: int Map.M(Name).t]

  let fix_meta_visitor =
    object
      inherit [_] map as super

      method! visit_Name () n =
        match Meta.find n meta_ref with
        | Some m -> Name (copy n ~meta:!m)
        | None -> Name n

      method! visit_t () ({meta= _; _} as r) =
        let ({meta; _} as r) = super#visit_t () r in
        (* Replace mutable refcounts with immutable refcounts. *)
        ( match Univ_map.find !meta mut_refcnt with
        | Some defs ->
            meta := Map.map defs ~f:( ! ) |> Univ_map.set !meta refcnt ;
            meta := Univ_map.remove !meta mut_refcnt
        | None -> () ) ;
        r
    end

  let shadow_check r =
    let relations_visitor =
      object
        inherit [_] reduce

        inherit [_] Util.set_monoid (module String)

        method! visit_Relation () r = Set.singleton (module String) r.r_name
      end
    in
    let alias_visitor relations =
      object (self)
        inherit [_] iter

        val aliases = Hash_set.create (module String) ()

        method check_name n =
          if Hash_set.mem aliases n then
            Error.(create "Duplicate alias." n [%sexp_of: string] |> raise)
          else if Set.mem relations n then
            Error.(
              create "Alias overlaps with relation." n [%sexp_of: string] |> raise)
          else Hash_set.add aliases n

        method check_alias () r =
          match r.node with
          | As (n, r') -> self#check_name n ; self#visit_t () r'
          | Relation r -> self#check_name r.r_name
          | r' ->
              self#visit_node () r' ;
              Logs.err (fun m -> m "Missing as: %a" pp_small r)

        method! visit_AList () (rk, rv) =
          self#check_alias () rk ; self#visit_t () rv

        method! visit_AHashIdx () (rk, rv, _) =
          self#check_alias () rk ; self#visit_t () rv

        method! visit_AOrderedIdx () (rk, rv, _) =
          self#check_alias () rk ; self#visit_t () rv

        method! visit_As () _ r =
          Logs.err (fun m -> m "Unexpected as: %a" pp_small r) ;
          self#visit_t () r
      end
    in
    let rels = relations_visitor#visit_t () r in
    (alias_visitor rels)#visit_t () r

  module Ctx = struct
    module T : sig
      type row =
        { rname: Name.t
        ; rstage: [`Run | `Compile]
        ; rmeta: Univ_map.t ref
        ; rrefs: int ref list }
      [@@deriving compare]

      type t = private row list [@@deriving sexp_of]

      val of_list : row list -> t
    end = struct
      type row =
        { rname: Name.t
        ; rstage: [`Run | `Compile]
        ; rmeta: Univ_map.t ref
        ; rrefs: int ref list }
      [@@deriving sexp_of]

      type t = row list [@@deriving sexp_of]

      let compare_row r1 r2 =
        [%compare: Name.t * [`Run | `Compile]] (r1.rname, r1.rstage)
          (r2.rname, r2.rstage)

      let of_list l =
        let l =
          List.map l ~f:(fun r ->
              {r with rname= Name.Meta.(set r.rname stage r.rstage)} )
        in
        let dups = List.find_all_dups l ~compare:compare_row in
        if List.length dups > 0 then (
          List.iter dups ~f:(fun r ->
              Logs.err (fun m -> m "Ambiguous name %a." Name.pp_with_stage r.rname)
          ) ;
          Error.(of_string "Ambiguous names." |> raise) ) ;
        l
    end

    include T

    let unscoped (c : t) =
      List.map (c :> row list) ~f:(fun r -> {r with rname= Name.unscoped r.rname})
      |> of_list

    (** Bind c2 over c1. *)
    let bind (c1 : t) (c2 : t) =
      let c2 = (c2 :> row list) in
      let c1 =
        List.filter
          (c1 :> row list)
          ~f:(fun r ->
            if List.mem c2 ~equal:[%compare.equal: row] r then (
              Logs.warn (fun m -> m "Shadowing of %a." Name.pp_with_stage r.rname) ;
              false )
            else true )
      in
      of_list (c1 @ c2)

    let merge (c1 : t) (c2 : t) : t = of_list ((c1 :> row list) @ (c2 :> row list))

    let merge_list (ls : t list) : t = List.concat (ls :> row list list) |> of_list

    let find (m : t) f = List.find (m :> row list) ~f:(fun r -> Name.O.(r.rname = f))

    let in_stage (c : t) s =
      List.filter
        (c :> row list)
        ~f:(fun r -> [%compare.equal: [`Run | `Compile]] r.rstage s)

    let incr_refs s (m : t) =
      let incr_ref {rrefs; _} = List.iter rrefs ~f:incr in
      in_stage m s |> List.iter ~f:incr_ref

    let to_schema p =
      let t =
        (* NOTE: Must first put type metadata back into names. *)
        Pred.to_type (fix_meta_visitor#visit_pred () p)
      in
      Option.map (Pred.to_name p) ~f:(Name.copy ~type_:(Some t))

    let of_defs s ps : _ * t =
      let visitor def meta =
        object
          inherit [_] map

          method! visit_Name () n =
            if Name.O.(n = def) then Name Name.Meta.(set n meta_ref meta)
            else Name n
        end
      in
      (* Create a list of definitions with fresh metadata refs. *)
      let metas, defs =
        List.map ps ~f:(fun p ->
            match to_schema p with
            | Some n ->
                (* If this is a definition point, annotate it with fresh metadata
                 and expose it in the context. *)
                let meta =
                  match Meta.find n meta_ref with
                  | Some m -> !m
                  | None -> Name.meta n
                in
                let meta = ref meta in
                let p = (visitor n meta)#visit_pred () p in
                (Some (n, meta), p)
            | None -> (None, p) )
        |> List.unzip
      in
      let ctx =
        List.filter_map metas
          ~f:
            (Option.map ~f:(fun (n, meta) ->
                 {rname= n; rmeta= meta; rrefs= []; rstage= s} ))
      in
      (defs, of_list ctx)

    let add_refcnts (ctx : t) : t * _ =
      let ctx, refcounts =
        List.map
          (ctx :> row list)
          ~f:(fun r ->
            let rc = ref 0 in
            ({r with rrefs= rc :: r.rrefs}, (r.rname, rc)) )
        |> List.unzip
      in
      let refcounts =
        Map.of_alist_multi (module Name) refcounts
        |> Map.mapi ~f:(fun ~key:n ~data ->
               match data with
               | [x] -> x
               | x :: _ ->
                   Logs.warn (fun m -> m "Output shadowing of %a." Name.pp n) ;
                   x
               | _ -> assert false )
      in
      (of_list ctx, refcounts)

    let rename (ctx : t) name : t =
      List.map
        ~f:(fun r -> {r with rname= copy ~relation:(Some name) r.rname})
        (ctx :> row list)
      |> of_list
  end

  (** Given a context containing names and a new name, determine which of the
     existing names corresponds and annotate the new name with the same type. *)
  let resolve_name ctx n =
    let m =
      match Ctx.find ctx n with
      | Some m -> m
      | None ->
          Error.raise
            (Error.create "Could not resolve." (n, ctx) [%sexp_of: t * Ctx.t])
    in
    List.iter m.rrefs ~f:incr ; m.rname

  let resolve_relation stage r =
    let schema = Option.value_exn r.r_schema in
    let _, ctx = List.map schema ~f:(fun n -> Name n) |> Ctx.of_defs stage in
    ctx

  let rec resolve_pred (ctx : Ctx.t) =
    let visitor =
      object
        inherit [_] endo

        method! visit_Name ctx _ n = Name (resolve_name ctx n)

        method! visit_Exists ctx _ r =
          let r', _ = resolve `Run ctx r in
          Exists r'

        method! visit_First ctx _ r =
          let r', _ = resolve `Run ctx r in
          First r'
      end
    in
    visitor#visit_pred ctx

  and resolve stage outer_ctx ({node; meta} as r) =
    let all_has_stage (ctx : Ctx.t) s =
      List.for_all (ctx :> Ctx.row list) ~f:(fun r -> r.Ctx.rstage = s)
    in
    let no_leakage (kctx : Ctx.t) (vctx : Ctx.t) =
      List.for_all
        (kctx :> Ctx.row list)
        ~f:(fun r -> not (List.mem ~equal:phys_equal (vctx :> Ctx.row list) r))
    in
    let rsame = resolve stage in
    let resolve' = function
      | Select (preds, r) ->
          let r, preds =
            let r, inner_ctx = rsame outer_ctx r in
            let ctx = Ctx.merge outer_ctx inner_ctx in
            (r, List.map preds ~f:(resolve_pred ctx))
          in
          let defs, ctx = Ctx.of_defs stage preds in
          (Select (defs, r), ctx)
      | Filter (pred, r) ->
          let r, value_ctx = rsame outer_ctx r in
          let pred = resolve_pred (Ctx.merge outer_ctx value_ctx) pred in
          (Filter (pred, r), value_ctx)
      | DepJoin ({d_lhs; d_rhs; d_alias} as d) ->
          let d_lhs, lctx = resolve `Compile outer_ctx d_lhs in
          let lctx = Ctx.rename lctx d_alias in
          let d_rhs, rctx = rsame (Ctx.bind outer_ctx lctx) d_rhs in
          (DepJoin {d with d_lhs; d_rhs}, Ctx.unscoped rctx)
      | Join {pred; r1; r2} ->
          let r1, inner_ctx1 = rsame outer_ctx r1 in
          let r2, inner_ctx2 = rsame outer_ctx r2 in
          let ctx = Ctx.merge_list [inner_ctx1; inner_ctx2; outer_ctx] in
          let pred = resolve_pred ctx pred in
          (Join {pred; r1; r2}, Ctx.merge inner_ctx1 inner_ctx2)
      | Relation r -> (Relation r, resolve_relation stage r |> Ctx.unscoped)
      | GroupBy (aggs, key, r) ->
          let r, inner_ctx = rsame outer_ctx r in
          let ctx = Ctx.merge outer_ctx inner_ctx in
          let aggs = List.map ~f:(resolve_pred ctx) aggs in
          let key = List.map key ~f:(resolve_name ctx) in
          let defs, ctx = Ctx.of_defs stage aggs in
          (GroupBy (defs, key, r), ctx)
      | Dedup r ->
          let r, inner_ctx = rsame outer_ctx r in
          (Dedup r, inner_ctx)
      | AEmpty -> (AEmpty, Ctx.of_list [])
      | AScalar p ->
          let p = resolve_pred outer_ctx p in
          let def, ctx =
            match Ctx.of_defs stage [p] with
            | [def], ctx -> (def, ctx)
            | _ -> assert false
          in
          (AScalar def, ctx)
      | AList (rk, rv) ->
          let rk, kctx = resolve `Compile outer_ctx rk in
          assert (all_has_stage kctx `Compile) ;
          let rv, vctx = rsame (Ctx.bind outer_ctx kctx) rv in
          assert (no_leakage kctx vctx) ;
          (AList (rk, rv), Ctx.unscoped vctx)
      | ATuple (ls, (Concat as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          (ATuple (ls, t), List.hd_exn ctxs)
      | ATuple (ls, ((Cross | Zip) as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          (ATuple (ls, t), Ctx.merge_list ctxs)
      | AHashIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          assert (all_has_stage key_ctx `Compile) ;
          let inner_ctx = Ctx.bind outer_ctx key_ctx in
          let vl, value_ctx = rsame inner_ctx l in
          let m = {m with lookup= List.map m.lookup ~f:(resolve_pred outer_ctx)} in
          (AHashIdx (r, vl, m), Ctx.(merge key_ctx value_ctx |> unscoped))
      | AOrderedIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          assert (all_has_stage key_ctx `Compile) ;
          let inner_ctx = Ctx.bind outer_ctx key_ctx in
          let vl, value_ctx = rsame inner_ctx l in
          let m =
            { m with
              lookup_low= resolve_pred outer_ctx m.lookup_low
            ; lookup_high= resolve_pred outer_ctx m.lookup_high }
          in
          (AOrderedIdx (r, vl, m), Ctx.(merge key_ctx value_ctx |> unscoped))
      | As (n, r) ->
          let r, ctx = rsame outer_ctx r in
          (As (n, r), Ctx.rename ctx n)
      | OrderBy {key; rel} ->
          let rel, inner_ctx = rsame outer_ctx rel in
          let key = List.map key ~f:(fun (p, o) -> (resolve_pred inner_ctx p, o)) in
          (OrderBy {key; rel}, inner_ctx)
    in
    let node, ctx =
      try resolve' node
      with exn ->
        let pp () x =
          Abslayout.pp Format.str_formatter x ;
          Format.flush_str_formatter ()
        in
        Exn.reraisef exn "Resolving: %a" pp r ()
    in
    let ctx, refcnts = Ctx.add_refcnts ctx in
    (* Logs.debug (fun m ->
     *     m "%a@ %a" Abslayout.pp r Sexp.pp_hum ([%sexp_of: Ctx.t] ctx) ) ; *)
    meta := Univ_map.set !meta mut_refcnt refcnts ;
    ({node; meta}, ctx)

  (** Annotate names in an algebra expression with types. *)
  let resolve ?(params = Set.empty (module Name)) r =
    shadow_check r ;
    let _, ctx =
      Ctx.of_defs `Run (Set.to_list params |> List.map ~f:(fun n -> Name n))
    in
    let r, ctx = resolve `Run ctx r in
    (* Ensure that all the outputs are referenced. *)
    Ctx.incr_refs `Run ctx ;
    fix_meta_visitor#visit_t () r
end
