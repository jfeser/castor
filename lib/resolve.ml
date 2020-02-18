open Collections
open Ast
open Abslayout_visitors
module A = Abslayout
module P = Pred.Infix
module N = Name

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

      val aliases = Hash_set.create (module String)

      method check_name n =
        if Hash_set.mem aliases n then
          Error.(create "Duplicate alias." n [%sexp_of: string] |> raise)
        else if Set.mem relations n then
          Error.(
            create "Alias overlaps with relation." n [%sexp_of: string] |> raise)
        else Hash_set.add aliases n

      method check_alias () r =
        match r.node with
        | As (n, r') ->
            self#check_name n;
            self#visit_t () r'
        | Relation r -> self#check_name r.r_name
        | _ ->
            self#visit_t () r;
            Log.err (fun m -> m "Missing as: %a" A.pp_small r)

      method! visit_AList () (rk, rv) =
        self#check_alias () rk;
        self#visit_t () rv

      method! visit_AHashIdx () h =
        self#check_name h.hi_scope;
        self#visit_hash_idx () h

      method! visit_AOrderedIdx () (rk, rv, _) =
        self#check_alias () rk;
        self#visit_t () rv

      method! visit_As () _ r =
        Log.err (fun m -> m "Unexpected as: %a" A.pp_small r);
        self#visit_t () r
    end
  in
  let rels = relations_visitor#visit_t () r in
  (alias_visitor rels)#visit_t () r

module Flag : sig
  type t [@@deriving sexp]

  val of_bool : bool -> t

  val bool_of : t -> bool

  val all : t list -> t
  (** If `t` is set, then all of `ts` will be set as well. *)

  val set : t -> unit
end = struct
  type t = { this : bool ref; deps : t list } [@@deriving sexp]

  let of_bool x = { this = ref x; deps = [] }

  let bool_of x = !(x.this)

  let rec set x =
    x.this := true;
    List.iter x.deps ~f:set

  let all xs = { this = ref false; deps = xs }
end

module Ctx = struct
  module T : sig
    type row = { rname : N.t; rstage : [ `Run | `Compile ]; rref : Flag.t }
    [@@deriving compare]

    type t = private row list [@@deriving sexp_of]

    val of_list : row list -> t
  end = struct
    type row = { rname : N.t; rstage : [ `Run | `Compile ]; rref : Flag.t }
    [@@deriving sexp_of]

    type t = row list [@@deriving sexp_of]

    let compare_row r1 r2 = [%compare: N.t] r1.rname r2.rname

    let of_list l =
      (* let l =
       *   List.map l ~f:(fun r ->
       *       { r with rname = N.Meta.(set r.rname stage r.rstage) })
       * in *)
      let dups = List.find_all_dups l ~compare:compare_row in
      if List.length dups > 0 then (
        List.iter dups ~f:(fun r ->
            Log.err (fun m -> m "Ambiguous name %a." N.pp_with_stage r.rname));
        Error.(of_string "Ambiguous names." |> raise) );
      l
  end

  include T

  let singleton n s =
    of_list [ { rname = n; rstage = s; rref = Flag.of_bool false } ]

  let unscoped (c : t) =
    List.map (c :> row list) ~f:(fun r -> { r with rname = N.unscoped r.rname })
    |> of_list

  let scoped s (c : t) =
    List.map (c :> row list) ~f:(fun r -> { r with rname = N.scoped s r.rname })
    |> of_list

  (** Bind c2 over c1. *)
  let bind (c1 : t) (c2 : t) =
    let c2 = (c2 :> row list) in
    let c1 =
      List.filter
        (c1 :> row list)
        ~f:(fun r ->
          if List.mem c2 ~equal:[%compare.equal: row] r then (
            Log.warn (fun m -> m "Shadowing of %a." N.pp_with_stage r.rname);
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
                   m "Name does not appear in all concat fields: %a" N.pp
                     r.rname);
               false )))
    |> List.map ~f:(List.sort ~compare:[%compare: row])
    |> List.transpose_exn
    |> List.map
         ~f:
           (List.reduce_exn ~f:(fun r r' ->
                { r with rref = Flag.all [ r.rref; r'.rref ] }))
    |> of_list

  let merge (c1 : t) (c2 : t) : t = of_list ((c1 :> row list) @ (c2 :> row list))

  (** This compensates for the overloaded hashidx and orderedidx key fields *)
  let merge_forgiving (c1 : t) (c2 : t) =
    (c1 :> row list) @ (c2 :> row list)
    |> List.dedup_and_sort ~compare:[%compare: row]
    |> of_list

  let merge_list (ls : t list) : t =
    List.concat (ls :> row list list) |> of_list

  let find (m : t) f = List.find (m :> row list) ~f:(fun r -> N.O.(r.rname = f))

  let in_stage (c : t) s =
    List.filter
      (c :> row list)
      ~f:(fun r -> [%compare.equal: [ `Run | `Compile ]] r.rstage s)

  let incr_refs s (m : t) =
    in_stage m s |> List.iter ~f:(fun { rref; _ } -> Flag.set rref)

  let to_schema p =
    Option.map (Pred.to_name p) ~f:(N.copy ~type_:(Some (Pred.to_type p)))

  let to_stage_map (c : t) =
    List.map (c :> row list) ~f:(fun r -> (r.rname, r.rstage))
    |> Map.of_alist_exn (module Name)

  (** Create a context from a selection list. *)
  let of_defs rstage (ps : 'a annot pred list) =
    List.filter_map ps ~f:to_schema
    |> List.map ~f:(fun n -> { rname = n; rref = Flag.of_bool false; rstage })
    |> of_list

  let refs (ctx : t) =
    List.map (ctx :> row list) ~f:(fun r -> (r.rname, Flag.bool_of r.rref))
    |> Map.of_alist_multi (module Name)
    |> Map.mapi ~f:(fun ~key:n ~data ->
           match data with
           | [ x ] -> x
           | x :: _ ->
               Log.warn (fun m -> m "Output shadowing of %a." N.pp n);
               x
           | _ -> assert false)
end

(** Given a context containing names and a new name, determine which of the
   existing names corresponds and annotate the new name with the same type. *)
let resolve_name ctx n =
  match Ctx.find ctx n with
  | Some m ->
      Flag.set m.rref;
      m.rname
  | None ->
      Error.raise
        (Error.create "Could not resolve." (n, ctx) [%sexp_of: N.t * Ctx.t])

let resolve_relation stage r =
  Option.value_exn ~message:"No schema annotation on relation."
    r.Relation.r_schema
  |> List.map ~f:P.name |> Ctx.of_defs stage

let as_ s r =
  {
    node = As (s, r);
    meta =
      object
        method outer = r.meta#outer

        method inner = Ctx.scoped s r.meta#inner
      end;
  }

let all_has_stage (ctx : Ctx.t) s =
  List.for_all (ctx :> Ctx.row list) ~f:(fun r -> Poly.(r.Ctx.rstage = s))

let rec resolve_pred resolve stage ctx =
  let resolve_noctx q =
    let q', _ = resolve stage ctx q in
    q'
  in
  function
  | Name n -> Name (resolve_name ctx n)
  | Exists r -> Exists (resolve_noctx r)
  | First r ->
      let r', ctx = resolve stage ctx r in
      Ctx.incr_refs stage ctx;
      First r'
  | p -> map_pred resolve_noctx (resolve_pred resolve stage ctx) p

let resolve_hash_idx resolve stage outer_ctx h =
  let r, kctx = resolve `Compile outer_ctx h.hi_keys in
  assert (all_has_stage kctx `Compile);
  let inner_ctx = Ctx.bind outer_ctx (Ctx.scoped h.hi_scope kctx) in
  let vl, vctx = resolve stage inner_ctx h.hi_values in
  let h =
    {
      h with
      hi_keys = r;
      hi_values = vl;
      hi_key_layout =
        Option.map h.hi_key_layout ~f:(fun q ->
            let q', _ = resolve `Compile inner_ctx q in
            q');
      hi_lookup = List.map h.hi_lookup ~f:(resolve_pred resolve stage outer_ctx);
    }
  in
  (AHashIdx h, Ctx.(merge_forgiving kctx vctx))

let resolve_ordered_idx resolve stage outer_ctx (r, l, m) =
  let scope = A.scope_exn r in
  let r = A.strip_scope r in
  let r, kctx = resolve `Compile outer_ctx r in
  assert (all_has_stage kctx `Compile);
  let inner_ctx = Ctx.bind outer_ctx (Ctx.scoped scope kctx) in
  let vl, vctx = resolve stage inner_ctx l in
  let resolve_bound b =
    Option.map ~f:(fun (p, b) -> (resolve_pred resolve stage outer_ctx p, b)) b
  in
  let m =
    {
      oi_key_layout =
        Option.map m.oi_key_layout ~f:(fun q ->
            let q', _ = resolve `Compile inner_ctx q in
            q');
      oi_lookup =
        List.map m.oi_lookup ~f:(fun (lb, ub) ->
            (resolve_bound lb, resolve_bound ub));
    }
  in
  (AOrderedIdx (as_ scope r, vl, m), Ctx.(merge_forgiving kctx vctx))

let resolve_open resolve stage outer_ctx =
  let rsame r = resolve stage r in
  let resolve_pred p = resolve_pred resolve p in
  function
  | Select (preds, r) ->
      let r, preds =
        let r, inner_ctx = rsame outer_ctx r in
        let ctx = Ctx.merge outer_ctx inner_ctx in
        (r, List.map preds ~f:(resolve_pred stage ctx))
      in
      let ctx = Ctx.of_defs stage preds in
      (Select (preds, r), ctx)
  | Filter (pred, r) ->
      let r, value_ctx = rsame outer_ctx r in
      let pred = resolve_pred stage (Ctx.merge outer_ctx value_ctx) pred in
      (Filter (pred, r), value_ctx)
  | DepJoin ({ d_lhs; d_rhs; d_alias } as d) ->
      let d_lhs, lctx = rsame outer_ctx d_lhs in
      let lctx = Ctx.scoped d_alias lctx in
      let d_rhs, rctx = rsame (Ctx.bind outer_ctx lctx) d_rhs in
      (DepJoin { d with d_lhs; d_rhs }, rctx)
  | Join { pred; r1; r2 } ->
      let r1, inner_ctx1 = rsame outer_ctx r1 in
      let r2, inner_ctx2 = rsame outer_ctx r2 in
      let ctx = Ctx.merge_list [ inner_ctx1; inner_ctx2; outer_ctx ] in
      let pred = resolve_pred stage ctx pred in
      (Join { pred; r1; r2 }, Ctx.merge inner_ctx1 inner_ctx2)
  | Relation r -> (Relation r, resolve_relation stage r)
  | Range (p, p') ->
      let p = resolve_pred stage outer_ctx p in
      let p' = resolve_pred stage outer_ctx p' in
      ( Range (p, p'),
        Ctx.singleton (N.create ~type_:(Pred.to_type p) "range") stage )
  | GroupBy (aggs, key, r) ->
      let r, inner_ctx = rsame outer_ctx r in
      let ctx = Ctx.merge outer_ctx inner_ctx in
      let aggs = List.map ~f:(resolve_pred stage ctx) aggs in
      let key = List.map key ~f:(resolve_name ctx) in
      let ctx = Ctx.of_defs stage aggs in
      (GroupBy (aggs, key, r), ctx)
  | Dedup r ->
      let r, inner_ctx = rsame outer_ctx r in
      (Dedup r, inner_ctx)
  | AEmpty -> (AEmpty, Ctx.of_list [])
  | AScalar p ->
      let p = resolve_pred stage outer_ctx p in
      let ctx = Ctx.of_defs stage [ p ] in
      (AScalar p, ctx)
  | AList (rk, rv) ->
      let scope = A.scope_exn rk in
      let rk = A.strip_scope rk in
      let rk, kctx = resolve `Compile outer_ctx rk in
      let rv, vctx = rsame (Ctx.bind outer_ctx (Ctx.scoped scope kctx)) rv in
      (AList (as_ scope rk, rv), vctx)
  | ATuple (ls, (Concat as t)) ->
      let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
      (ATuple (ls, t), Ctx.concat ctxs)
  | ATuple (ls, ((Cross | Zip) as t)) ->
      let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
      (ATuple (ls, t), Ctx.merge_list ctxs)
  | As _ -> Error.(createf "Unexpected as." |> raise)
  | OrderBy { key; rel } ->
      let rel, inner_ctx = rsame outer_ctx rel in
      let key =
        List.map key ~f:(fun (p, o) -> (resolve_pred stage inner_ctx p, o))
      in
      (OrderBy { key; rel }, inner_ctx)
  | AOrderedIdx o -> resolve_ordered_idx resolve stage outer_ctx o
  | AHashIdx h -> resolve_hash_idx resolve stage outer_ctx h

exception Inner_resolve_error of unit annot * exn

exception Resolve_error of unit annot * unit annot * exn [@@deriving sexp]

let rec resolve stage outer_ctx r =
  let node, ctx =
    try resolve_open resolve stage outer_ctx r.node
    with exn -> raise (Inner_resolve_error (A.strip_meta r, exn))
  in
  let ctx = Ctx.unscoped ctx in
  ( {
      node;
      meta =
        object
          method outer = outer_ctx

          method inner = ctx
        end;
    },
    ctx )

let stage =
  let merge =
    Map.merge ~f:(fun ~key ->
      function
      | `Left x | `Right x -> Some x
      | `Both (x, x') when Poly.(x = x') -> Some x
      | `Both (x, x') ->
          Error.create "Ambiguous stage" key [%sexp_of: Name.t] |> Error.raise)
  in
  let rec annot r =
    {
      node = query r.node;
      meta =
        object
          method stage =
            merge
              (Ctx.to_stage_map r.meta#outer)
              (Ctx.to_stage_map r.meta#inner)

          method inner = r.meta#inner
        end;
    }
  and query q = map_query annot pred q
  and pred p = map_pred annot pred p in
  annot

let refs =
  let rec annot r =
    {
      node = query r.node;
      meta =
        object
          method stage = r.meta#stage

          method refs = Ctx.refs r.meta#inner
        end;
    }
  and query q = map_query annot pred q
  and pred p = map_pred annot pred p in
  annot

(** Annotate names in an algebra expression with types. *)
let resolve ?(params = Set.empty (module Name)) r =
  let r = A.strip_meta r in
  shadow_check r;
  let r, ctx =
    let param_ctx =
      Ctx.of_defs `Run (Set.to_list params |> List.map ~f:P.name)
    in
    try resolve `Run param_ctx r
    with Inner_resolve_error (r', exn) -> raise (Resolve_error (r, r', exn))
  in

  (* Ensure that all the outputs are referenced. *)
  Ctx.incr_refs `Run ctx;

  (* Add metadata *)
  stage r |> refs
