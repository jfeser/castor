open Core
open Ast
module V = Visitors
module A = Abslayout
module P = Pred.Infix
module N = Name

type resolved = unit

type 'a meta =
  < refs : bool Map.M(Name).t
  ; stage : [ `Compile | `Run ] Map.M(Name).t
  ; resolved : resolved
  ; meta : 'a >

type inner_error =
  [ `Ambiguous_names of Name.t list
  | `Ambiguous_stage of Name.t
  | `Unbound of Name.t * Name.t list ]
[@@deriving sexp]

type error = [ `Resolve of Ast.t * inner_error ] [@@deriving sexp]

let pp_inner_err fmt = function
  | `Ambiguous_names ns ->
      Fmt.pf fmt "Ambiguous names: %a" (Fmt.Dump.list Name.pp) ns
  | `Ambiguous_stage n -> Fmt.pf fmt "Ambiguous stage: %a" Name.pp n
  | `Unbound (n, _) -> Fmt.pf fmt "Unbound name: %a" Name.pp n

let pp_err f fmt = function
  | `Resolve (r, err) ->
      Fmt.pf fmt "Resolving failed:@ %a@ with error:@ %a@." Abslayout_pp.pp r
        pp_inner_err err
  | x -> f fmt x

exception Inner_resolve_error of inner_error [@@deriving sexp]
exception Resolve_error of error [@@deriving sexp]

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
      let dups =
        List.find_all_dups l ~compare:compare_row
        |> List.map ~f:(fun r -> r.rname)
      in
      if
        List.length dups > 0
        && List.exists dups ~f:(fun n -> String.(Name.name n <> "dummy"))
      then raise @@ Inner_resolve_error (`Ambiguous_names dups);
      l
  end

  include T

  let names (c : t) = List.map (c :> row list) ~f:(fun r -> r.rname)

  let of_names rstage ns =
    List.map ns ~f:(fun n -> { rname = n; rref = Flag.of_bool false; rstage })
    |> of_list

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
            Log.warn (fun m -> m "Shadowing of %a." N.pp r.rname);
            false)
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
               Log.warn (fun m ->
                   m "Name does not appear in all concat fields: %a" N.pp
                     r.rname);
               false)))
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

  let to_stage_map (c : t) =
    List.map (c :> row list) ~f:(fun r -> (r.rname, r.rstage))
    |> Map.of_alist_exn (module Name)

  (** Create a context from a selection list. *)
  let of_select_list rstage (ps : 'a annot pred Select_list.t) =
    Select_list.to_list ps
    |> List.map ~f:(fun (p, n) -> N.create ~type_:(Pred.to_type p) n)
    |> of_names rstage

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
  | None -> raise @@ Inner_resolve_error (`Unbound (n, Ctx.names ctx))

let resolve_relation stage r = Relation.schema r |> Ctx.of_names stage

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
  | p -> V.Map.pred resolve_noctx (resolve_pred resolve stage ctx) p

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

let resolve_ordered_idx resolve stage outer_ctx o =
  let r, kctx = resolve `Compile outer_ctx o.oi_keys in
  assert (all_has_stage kctx `Compile);
  let inner_ctx = Ctx.bind outer_ctx (Ctx.scoped o.oi_scope kctx) in
  let vl, vctx = resolve stage inner_ctx o.oi_values in
  let resolve_bound b =
    Option.map ~f:(fun (p, b) -> (resolve_pred resolve stage outer_ctx p, b)) b
  in
  let o =
    {
      o with
      oi_keys = r;
      oi_values = vl;
      oi_key_layout =
        Option.map o.oi_key_layout ~f:(fun q ->
            let q', _ = resolve `Compile inner_ctx q in
            q');
      oi_lookup =
        List.map o.oi_lookup ~f:(fun (lb, ub) ->
            (resolve_bound lb, resolve_bound ub));
    }
  in
  (AOrderedIdx o, Ctx.(merge_forgiving kctx vctx))

let resolve_open resolve stage outer_ctx =
  let rsame r = resolve stage r in
  let resolve_pred p = resolve_pred resolve p in
  function
  | Select (preds, r) ->
      let r, preds =
        let r, inner_ctx = rsame outer_ctx r in
        let ctx = Ctx.merge outer_ctx inner_ctx in
        (r, Select_list.map preds ~f:(fun p _ -> resolve_pred stage ctx p))
      in
      let ctx = Ctx.of_select_list stage preds in
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
      let aggs =
        Select_list.map ~f:(fun p _ -> resolve_pred stage ctx p) aggs
      in
      let key = List.map key ~f:(resolve_name ctx) in
      let ctx = Ctx.of_select_list stage aggs in
      (GroupBy (aggs, key, r), ctx)
  | Dedup r ->
      let r, inner_ctx = rsame outer_ctx r in
      (Dedup r, inner_ctx)
  | AEmpty -> (AEmpty, Ctx.of_list [])
  | AScalar s ->
      let p = resolve_pred stage outer_ctx s.s_pred in
      let ctx = Ctx.of_select_list stage [ (p, s.s_name) ] in
      (AScalar { s with s_pred = p }, ctx)
  | AList ({ l_keys = rk; l_scope = scope; l_values = rv } as l) ->
      let rk, kctx = resolve `Compile outer_ctx rk in
      let rv, vctx = rsame (Ctx.bind outer_ctx (Ctx.scoped scope kctx)) rv in
      (AList { l with l_keys = rk; l_values = rv }, vctx)
  | ATuple (ls, (Concat as t)) ->
      let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
      (ATuple (ls, t), Ctx.concat ctxs)
  | ATuple (ls, ((Cross | Zip) as t)) ->
      let ls, ctxs = List.map ls ~f:(rsame outer_ctx) |> List.unzip in
      (ATuple (ls, t), Ctx.merge_list ctxs)
  | OrderBy { key; rel } ->
      let rel, inner_ctx = rsame outer_ctx rel in
      let key =
        List.map key ~f:(fun (p, o) -> (resolve_pred stage inner_ctx p, o))
      in
      (OrderBy { key; rel }, inner_ctx)
  | AOrderedIdx o -> resolve_ordered_idx resolve stage outer_ctx o
  | AHashIdx h -> resolve_hash_idx resolve stage outer_ctx h

let rec resolve stage outer_ctx r =
  let node, ctx = resolve_open resolve stage outer_ctx r.node in
  let ctx = Ctx.unscoped ctx in
  let meta =
    object
      method outer = outer_ctx
      method inner = ctx
      method meta = r.meta
    end
  in
  ({ node; meta }, ctx)

let stage =
  let merge =
    Map.merge ~f:(fun ~key -> function
      | `Left x | `Right x -> Some x
      | `Both (x, x') when Poly.(x = x') -> Some x
      | `Both _ -> raise (Inner_resolve_error (`Ambiguous_stage key)))
  in
  let rec annot r =
    let meta =
      object
        method stage =
          merge (Ctx.to_stage_map r.meta#outer) (Ctx.to_stage_map r.meta#inner)

        method inner = r.meta#inner
        method meta = r.meta#meta
      end
    in
    { node = query r.node; meta }
  and query q = V.Map.query annot pred q
  and pred p = V.Map.pred annot pred p in
  annot

let refs =
  let rec annot r =
    let meta =
      object
        method stage = r.meta#stage
        method refs = Ctx.refs r.meta#inner
        method resolved = ()
        method meta = r.meta#meta
      end
    in
    { node = query r.node; meta }
  and query q = V.Map.query annot pred q
  and pred p = V.Map.pred annot pred p in
  annot

(** Annotate names in an algebra expression with types. *)
let resolve_exn ~params r =
  let param_ctx = Ctx.of_names `Run @@ Set.to_list params in
  let r, ctx =
    try resolve `Run param_ctx r
    with Inner_resolve_error x ->
      raise (Resolve_error (`Resolve (Ast.strip_meta r, x)))
  in

  (* Ensure that all the outputs are referenced. *)
  Ctx.incr_refs `Run ctx;

  (* Add metadata *)
  stage r |> refs

let resolve ~params r =
  try Ok (resolve_exn ~params r) with Resolve_error (#error as x) -> Error x
