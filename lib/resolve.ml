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
    let alias_visitor =
      object (self)
        inherit [_] iter

        method check_name seen n =
          if Hash_set.mem seen n then
            Error.(create "Duplicate relation." n [%sexp_of: string] |> raise)
          else Hash_set.add seen n

        method check_alias seen r =
          match r.node with
          | As (n, _) -> self#check_name seen n
          | Relation r -> self#check_name seen r.r_name
          | _ -> Logs.err (fun m -> m "Missing as: %a" pp_small r)

        method! visit_AList seen (rk, _) = self#check_alias seen rk

        method! visit_AHashIdx seen (rk, _, _) = self#check_alias seen rk

        method! visit_AOrderedIdx seen (rk, _, _) = self#check_alias seen rk

        method! visit_Relation seen rel = self#check_name seen rel.r_name

        method! visit_As seen name r =
          match r.node with
          | Relation _ -> self#check_name seen name
          | _ -> Logs.warn (fun m -> m "Unexpected as: %a" pp_small r)
      end
    in
    let seen = Hash_set.create (module String) () in
    alias_visitor#visit_t seen r

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
        List.find_all_dups l ~compare:compare_row
        |> List.iter ~f:(fun r ->
               Logs.warn (fun m -> m "Shadowing of %a." Name.pp_with_stage r.rname)
           ) ;
        l
    end

    include T

    (** Bind c2 over c1. *)
    let bind (c1 : t) (c2 : t) =
      let c2 = (c2 :> row list) in
      let c1 =
        List.filter
          (c1 :> row list)
          ~f:(fun r -> not (List.mem c2 ~equal:[%compare.equal: row] r))
      in
      of_list (c1 @ c2)

    let merge (c1 : t) (c2 : t) : t = of_list ((c1 :> row list) @ (c2 :> row list))

    let merge_list (ls : t list) : t = List.concat (ls :> row list list) |> of_list

    let find_name (m : t) rel f =
      let m = (m :> row list) in
      let names =
        match rel with
        | `Rel r ->
            let k = Name.create ~relation:r f in
            List.filter m ~f:(fun r -> Name.O.(r.rname = k))
        | `NoRel ->
            let k = Name.create f in
            List.filter m ~f:(fun r -> Name.O.(r.rname = k))
        | `AnyRel -> List.filter m ~f:(fun r -> String.(name r.rname = f))
      in
      match names with
      | [] -> None
      | [r] -> Some r
      | r' :: r'' :: _ ->
          Logs.err (fun m ->
              m "Ambiguous name %s: could refer to %a or %a" f Name.pp r'.rname
                Name.pp r''.rname ) ;
          None

    let in_stage (c : t) s =
      List.filter
        (c :> row list)
        ~f:(fun r -> [%compare.equal: [`Run | `Compile]] r.rstage s)

    let find s (m : t) rel f =
      match
        ( find_name (in_stage m `Run |> of_list) rel f
        , find_name (in_stage m `Compile |> of_list) rel f )
      with
      | Some v, None | None, Some v -> Some v
      | None, None -> None
      | Some mr, Some mc -> (
          let to_name rel f =
            match rel with
            | `Rel r -> Name.create ~relation:r f
            | _ -> Name.create f
          in
          Logs.info (fun m ->
              m "Cross-stage shadowing of %a." Name.pp (to_name rel f) ) ;
          match s with `Run -> Some mr | `Compile -> Some mc )

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
  let resolve_name s ctx n =
    let could_not_resolve =
      Error.create "Could not resolve." (n, ctx) [%sexp_of: t * Ctx.t]
    in
    let m =
      match rel n with
      | Some r -> (
        (* If the name has a relational part, then it must exactly match a
             name in the set. *)
        match Ctx.find s ctx (`Rel r) (name n) with
        | Some m -> m
        | None -> Error.raise could_not_resolve )
      | None -> (
        (* If the name has no relational part, first try to resolve it to
             another name that also lacks a relational part. *)
        match Ctx.find s ctx `NoRel (name n) with
        | Some m -> m
        | None -> (
          (* If no such name exists, then resolve it only if there is exactly
               one name where the field parts match. Otherwise the name is
               ambiguous. *)
          match Ctx.find s ctx `AnyRel (name n) with
          | Some m -> m
          | None -> Error.raise could_not_resolve ) )
    in
    List.iter m.rrefs ~f:incr ; m.rname

  let resolve_relation stage r =
    let schema = Option.value_exn r.r_schema in
    let _, ctx = List.map schema ~f:(fun n -> Name n) |> Ctx.of_defs stage in
    ctx

  let rec resolve_pred stage (ctx : Ctx.t) =
    let visitor =
      object
        inherit [_] endo

        method! visit_Name ctx _ n = Name (resolve_name stage ctx n)

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
            (r, List.map preds ~f:(resolve_pred stage ctx))
          in
          let defs, ctx = Ctx.of_defs stage preds in
          (Select (defs, r), ctx)
      | Filter (pred, r) ->
          let r, value_ctx = rsame outer_ctx r in
          let pred = resolve_pred stage (Ctx.merge outer_ctx value_ctx) pred in
          (Filter (pred, r), value_ctx)
      | Join {pred; r1; r2} ->
          let r1, inner_ctx1 = rsame outer_ctx r1 in
          let r2, inner_ctx2 = rsame outer_ctx r2 in
          let ctx = Ctx.merge_list [inner_ctx1; inner_ctx2; outer_ctx] in
          let pred = resolve_pred stage ctx pred in
          (Join {pred; r1; r2}, Ctx.merge inner_ctx1 inner_ctx2)
      | Relation r -> (Relation r, resolve_relation stage r)
      | GroupBy (aggs, key, r) ->
          let r, inner_ctx = rsame outer_ctx r in
          let ctx = Ctx.merge outer_ctx inner_ctx in
          let aggs = List.map ~f:(resolve_pred stage ctx) aggs in
          let key = List.map key ~f:(resolve_name stage ctx) in
          let defs, ctx = Ctx.of_defs stage aggs in
          (GroupBy (defs, key, r), ctx)
      | Dedup r ->
          let r, inner_ctx = rsame outer_ctx r in
          (Dedup r, inner_ctx)
      | AEmpty -> (AEmpty, Ctx.of_list [])
      | AScalar p ->
          let p = resolve_pred `Compile outer_ctx p in
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
          (AList (rk, rv), vctx)
      | ATuple (ls, (Zip as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          (ATuple (ls, t), Ctx.merge_list ctxs)
      | ATuple (ls, (Concat as t)) ->
          let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
          (ATuple (ls, t), List.hd_exn ctxs)
      | ATuple (ls, (Cross as t)) ->
          let ls, ctx =
            List.fold_left ls
              ~init:([], Ctx.of_list [])
              ~f:(fun (ls, ctx) l ->
                let l, ctx' = rsame (Ctx.bind outer_ctx ctx) l in
                (l :: ls, Ctx.merge ctx ctx') )
          in
          (ATuple (List.rev ls, t), ctx)
      | AHashIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          assert (all_has_stage key_ctx `Compile) ;
          let inner_ctx = Ctx.bind outer_ctx key_ctx in
          let vl, value_ctx = rsame inner_ctx l in
          let m =
            {m with lookup= List.map m.lookup ~f:(resolve_pred stage outer_ctx)}
          in
          (AHashIdx (r, vl, m), Ctx.merge key_ctx value_ctx)
      | AOrderedIdx (r, l, m) ->
          let r, key_ctx = resolve `Compile outer_ctx r in
          assert (all_has_stage key_ctx `Compile) ;
          let inner_ctx = Ctx.bind outer_ctx key_ctx in
          let vl, value_ctx = rsame inner_ctx l in
          let m =
            { m with
              lookup_low= resolve_pred stage outer_ctx m.lookup_low
            ; lookup_high= resolve_pred stage outer_ctx m.lookup_high }
          in
          (AOrderedIdx (r, vl, m), Ctx.merge key_ctx value_ctx)
      | As (n, r) ->
          let r, ctx = rsame outer_ctx r in
          (As (n, r), Ctx.rename ctx n)
      | OrderBy {key; rel} ->
          let rel, inner_ctx = rsame outer_ctx rel in
          let key =
            List.map key ~f:(fun (p, o) -> (resolve_pred stage inner_ctx p, o))
          in
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
