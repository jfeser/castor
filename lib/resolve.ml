open! Core
open Abslayout0
open Collections
module N = Name

let meta_ref = Univ_map.Key.create ~name:"meta-ref" [%sexp_of: Univ_map.t ref]

let mut_refcnt =
  Univ_map.Key.create ~name:"mut-refcnt" [%sexp_of: int ref Map.M(Name).t]

let fix_meta_visitor =
  object
    inherit [_] map as super

    method! visit_Name () n =
      match N.Meta.find n meta_ref with
      | Some m -> Name (N.copy n ~meta:!m)
      | None -> Name n

    method! visit_t () ({meta= _; _} as r) =
      let ({meta; _} as r) = super#visit_t () r in
      (* Replace mutable refcounts with immutable refcounts. *)
      ( match Univ_map.find !meta mut_refcnt with
      | Some defs ->
          meta := Map.map defs ~f:( ! ) |> Univ_map.set !meta Meta.refcnt ;
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
            Log.err (fun m -> m "Missing as: %a" pp_small r)

      method! visit_AList () (rk, rv) = self#check_alias () rk ; self#visit_t () rv

      method! visit_AHashIdx () h =
        self#check_name h.hi_scope ; self#visit_hash_idx () h

      method! visit_AOrderedIdx () (rk, rv, _) =
        self#check_alias () rk ; self#visit_t () rv

      method! visit_As () _ r =
        Log.err (fun m -> m "Unexpected as: %a" pp_small r) ;
        self#visit_t () r
    end
  in
  let rels = relations_visitor#visit_t () r in
  (alias_visitor rels)#visit_t () r

module Ctx = struct
  module T : sig
    type row = {rname: Name.t; rstage: [`Run | `Compile]; rrefs: int ref list}
    [@@deriving compare]

    type t = private row list [@@deriving sexp_of]

    val of_list : row list -> t
  end = struct
    type row = {rname: Name.t; rstage: [`Run | `Compile]; rrefs: int ref list}
    [@@deriving sexp_of]

    type t = row list [@@deriving sexp_of]

    let compare_row r1 r2 = [%compare: Name.t] r1.rname r2.rname

    let of_list l =
      let l =
        List.map l ~f:(fun r ->
            {r with rname= Name.Meta.(set r.rname stage r.rstage)})
      in
      let dups = List.find_all_dups l ~compare:compare_row in
      if List.length dups > 0 then (
        List.iter dups ~f:(fun r ->
            Log.err (fun m -> m "Ambiguous name %a." Name.pp_with_stage r.rname)) ;
        Error.(of_string "Ambiguous names." |> raise) ) ;
      l
  end

  include T

  let singleton n s = of_list [{rname= n; rstage= s; rrefs= []}]

  let unscoped (c : t) =
    List.map (c :> row list) ~f:(fun r -> {r with rname= Name.unscoped r.rname})
    |> of_list

  let scoped s (c : t) =
    List.map (c :> row list) ~f:(fun r -> {r with rname= Name.scoped s r.rname})
    |> of_list

  (** Bind c2 over c1. *)
  let bind (c1 : t) (c2 : t) =
    let c2 = (c2 :> row list) in
    let c1 =
      List.filter
        (c1 :> row list)
        ~f:(fun r ->
          if List.mem c2 ~equal:[%compare.equal: row] r then (
            Log.warn (fun m -> m "Shadowing of %a." Name.pp_with_stage r.rname) ;
            false )
          else true)
    in
    of_list (c1 @ c2)

  let concat (cs : t list) =
    let cs = (cs :> row list list) in
    let inter_names =
      List.map cs ~f:(fun c ->
          List.map c ~f:(fun r -> r.rname) |> Set.of_list (module Name))
      |> List.reduce_exn ~f:Set.inter
    in
    List.map cs
      ~f:
        (List.filter ~f:(fun r ->
             if Set.mem inter_names r.rname then true
             else (
               Logs.warn (fun m ->
                   m "Name does not appear in all concat fields: %a" Name.pp r.rname) ;
               false )))
    |> List.map ~f:(List.sort ~compare:[%compare: row])
    |> List.transpose_exn
    |> List.map
         ~f:(List.reduce_exn ~f:(fun r r' -> {r with rrefs= r.rrefs @ r'.rrefs}))
    |> of_list

  let merge (c1 : t) (c2 : t) : t = of_list ((c1 :> row list) @ (c2 :> row list))

  (** This compensates for the overloaded hashidx and orderedidx key fields *)
  let merge_forgiving (c1 : t) (c2 : t) =
    (c1 :> row list) @ (c2 :> row list)
    |> List.dedup_and_sort ~compare:[%compare: row]
    |> of_list

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
          if Name.O.(n = def) then Name Name.Meta.(set n meta_ref meta) else Name n
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
                match N.Meta.find n meta_ref with
                | Some m -> !m
                | None -> Name.meta n
              in
              let meta = ref meta in
              let p = (visitor n meta)#visit_pred () p in
              (Some (n, meta), p)
          | None -> (None, p))
      |> List.unzip
    in
    let ctx =
      List.filter_map metas
        ~f:(Option.map ~f:(fun (n, _) -> {rname= n; rrefs= []; rstage= s}))
    in
    (defs, of_list ctx)

  let add_refcnts (ctx : t) : t * _ =
    let ctx, refcounts =
      List.map
        (ctx :> row list)
        ~f:(fun r ->
          let rc = ref 0 in
          ({r with rrefs= rc :: r.rrefs}, (r.rname, rc)))
      |> List.unzip
    in
    let refcounts =
      Map.of_alist_multi (module Name) refcounts
      |> Map.mapi ~f:(fun ~key:n ~data ->
             match data with
             | [x] -> x
             | x :: _ ->
                 Log.warn (fun m -> m "Output shadowing of %a." Name.pp n) ;
                 x
             | _ -> assert false)
    in
    (of_list ctx, refcounts)
end

(** Given a context containing names and a new name, determine which of the
     existing names corresponds and annotate the new name with the same type. *)
let resolve_name ctx n =
  let m =
    match Ctx.find ctx n with
    | Some m -> m
    | None ->
        Error.raise
          (Error.create "Could not resolve." (n, ctx) [%sexp_of: N.t * Ctx.t])
  in
  List.iter m.rrefs ~f:incr ; m.rname

let resolve_relation stage r =
  let schema =
    Option.value_exn ~message:"No schema annotation on relation." r.r_schema
  in
  let _, ctx = List.map schema ~f:(fun n -> Name n) |> Ctx.of_defs stage in
  ctx

let rec resolve_pred stage (ctx : Ctx.t) =
  let visitor =
    object
      inherit [_] endo

      method! visit_Name ctx _ n = Name (resolve_name ctx n)

      method! visit_Exists ctx _ r =
        let r', _ = resolve stage ctx r in
        Exists r'

      method! visit_First ctx _ r =
        let r', ctx = resolve stage ctx r in
        Ctx.incr_refs stage ctx ; First r'
    end
  in
  visitor#visit_pred ctx

and resolve stage outer_ctx ({node; meta} as r) =
  let all_has_stage (ctx : Ctx.t) s =
    List.for_all (ctx :> Ctx.row list) ~f:(fun r -> r.Ctx.rstage = s)
  in
  let rsame = resolve stage in
  let as_ s r =
    let rc =
      Meta.find_exn r mut_refcnt |> Map.to_alist
      |> List.map ~f:(fun (n, c) -> (Name.scoped s n, c))
      |> Map.of_alist_exn (module Name)
    in
    Meta.set {node= As (s, r); meta= Meta.empty ()} mut_refcnt rc
  in
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
    | DepJoin ({d_lhs; d_rhs; d_alias} as d) ->
        let d_lhs, lctx = rsame outer_ctx d_lhs in
        let lctx = Ctx.scoped d_alias lctx in
        let d_rhs, rctx = rsame (Ctx.bind outer_ctx lctx) d_rhs in
        (DepJoin {d with d_lhs; d_rhs}, rctx)
    | Join {pred; r1; r2} ->
        let r1, inner_ctx1 = rsame outer_ctx r1 in
        let r2, inner_ctx2 = rsame outer_ctx r2 in
        let ctx = Ctx.merge_list [inner_ctx1; inner_ctx2; outer_ctx] in
        let pred = resolve_pred stage ctx pred in
        (Join {pred; r1; r2}, Ctx.merge inner_ctx1 inner_ctx2)
    | Relation r -> (Relation r, resolve_relation stage r)
    | Range (p, p') ->
        let p = resolve_pred stage outer_ctx p in
        let p' = resolve_pred stage outer_ctx p' in
        ( Range (p, p')
        , Ctx.singleton (Name.create ~type_:(Pred.to_type p) "range") stage )
    | GroupBy (aggs, key, r) ->
        let r, inner_ctx = rsame outer_ctx r in
        let ctx = Ctx.merge outer_ctx inner_ctx in
        let aggs = List.map ~f:(resolve_pred stage ctx) aggs in
        let key = List.map key ~f:(resolve_name ctx) in
        let defs, ctx = Ctx.of_defs stage aggs in
        (GroupBy (defs, key, r), ctx)
    | Dedup r ->
        let r, inner_ctx = rsame outer_ctx r in
        (Dedup r, inner_ctx)
    | AEmpty -> (AEmpty, Ctx.of_list [])
    | AScalar p ->
        let p = resolve_pred stage outer_ctx p in
        let def, ctx =
          match Ctx.of_defs stage [p] with
          | [def], ctx -> (def, ctx)
          | _ -> assert false
        in
        (AScalar def, ctx)
    | AList (rk, rv) ->
        let scope = scope_exn rk in
        let rk = strip_scope rk in
        let rk, kctx = resolve `Compile outer_ctx rk in
        let rv, vctx = rsame (Ctx.bind outer_ctx (Ctx.scoped scope kctx)) rv in
        (AList (as_ scope rk, rv), vctx)
    | ATuple (ls, (Concat as t)) ->
        let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
        (ATuple (ls, t), Ctx.concat ctxs)
    | ATuple (ls, ((Cross | Zip) as t)) ->
        let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
        (ATuple (ls, t), Ctx.merge_list ctxs)
    | AHashIdx h ->
        let r, kctx = resolve `Compile outer_ctx h.hi_keys in
        assert (all_has_stage kctx `Compile) ;
        let inner_ctx = Ctx.bind outer_ctx (Ctx.scoped h.hi_scope kctx) in
        let vl, vctx = rsame inner_ctx h.hi_values in
        let h =
          { h with
            hi_keys= r
          ; hi_values= vl
          ; hi_lookup= List.map h.hi_lookup ~f:(resolve_pred stage outer_ctx) }
        in
        (AHashIdx h, Ctx.(merge_forgiving kctx vctx))
    | AOrderedIdx (r, l, m) ->
        let scope = scope_exn r in
        let r = strip_scope r in
        let r, kctx = resolve `Compile outer_ctx r in
        assert (all_has_stage kctx `Compile) ;
        let inner_ctx = Ctx.bind outer_ctx (Ctx.scoped scope kctx) in
        let vl, vctx = rsame inner_ctx l in
        let resolve_bound =
          Option.map ~f:(fun (p, b) -> (resolve_pred stage outer_ctx p, b))
        in
        let m =
          { m with
            oi_lookup=
              List.map m.oi_lookup ~f:(fun (lb, ub) ->
                  (resolve_bound lb, resolve_bound ub)) }
        in
        (AOrderedIdx (as_ scope r, vl, m), Ctx.(merge_forgiving kctx vctx))
    | As _ -> Error.(createf "Unexpected as." |> raise)
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
        pp Format.str_formatter x ;
        Format.flush_str_formatter ()
      in
      Exn.reraisef exn "Resolving: %a" pp r ()
  in
  let ctx = Ctx.unscoped ctx in
  let ctx, refcnts = Ctx.add_refcnts ctx in
  (* Log.debug (fun m ->
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
