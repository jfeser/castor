open Core
open Ast
open Abslayout_fold
module V = Visitors
open Collections
module A = Constructors.Annot
module I = Abs_int
include (val Log.make "castor.type")

exception TypeError of Error.t [@@deriving sexp]

type t = Ast.type_ [@@deriving compare, sexp]

let least_general_of_primtype = function
  | Prim_type.IntT { nullable } -> IntT { range = I.bot; nullable }
  | NullT -> NullT
  | DateT { nullable } -> DateT { range = I.bot; nullable }
  | FixedT { nullable } -> FixedT { value = Abs_fixed.bot; nullable }
  | StringT { nullable; _ } -> StringT { nchars = I.bot; nullable }
  | BoolT { nullable } -> BoolT { nullable }
  | TupleT _ | VoidT -> failwith "Not a layout type."

let rec unify_exn t1 t2 =
  let fail m =
    let err =
      Error.create
        (Printf.sprintf "Unification failed: %s" m)
        (t1, t2) [%sexp_of: t * t]
    in
    raise (TypeError err)
  in
  match (t1, t2) with
  | NullT, NullT -> NullT
  | IntT { range = b1; nullable = n1 }, IntT { range = b2; nullable = n2 } ->
      IntT { range = I.O.(b1 || b2); nullable = n1 || n2 }
  | DateT { range = b1; nullable = n1 }, DateT { range = b2; nullable = n2 } ->
      DateT { range = I.O.(b1 || b2); nullable = n1 || n2 }
  | FixedT { value = v1; nullable = n1 }, FixedT { value = v2; nullable = n2 }
    ->
      FixedT { value = Abs_fixed.join v1 v2; nullable = n1 || n2 }
  | IntT x, NullT | NullT, IntT x -> IntT { x with nullable = true }
  | DateT x, NullT | NullT, DateT x -> DateT { x with nullable = true }
  | FixedT x, NullT | NullT, FixedT x -> FixedT { x with nullable = true }
  | BoolT { nullable = n1 }, BoolT { nullable = n2 } ->
      BoolT { nullable = n1 || n2 }
  | BoolT _, NullT | NullT, BoolT _ -> BoolT { nullable = true }
  | ( StringT { nchars = b1; nullable = n1 },
      StringT { nchars = b2; nullable = n2 } ) ->
      StringT { nchars = I.O.(b1 || b2); nullable = n1 || n2 }
  | StringT x, NullT | NullT, StringT x -> StringT { x with nullable = true }
  | TupleT (e1s, { kind = k1 }), TupleT (e2s, { kind = k2 }) when Poly.(k1 = k2)
    ->
      let elem_ts =
        match List.map2 e1s e2s ~f:unify_exn with
        | Ok ts -> ts
        | Unequal_lengths -> fail "Different number of columns."
      in
      TupleT (elem_ts, { kind = k1 })
  | ListT (et1, { count = c1 }), ListT (et2, { count = c2 }) ->
      ListT (unify_exn et1 et2, { count = I.O.(c1 || c2) })
  | ( OrderedIdxT (k1, v1, { key_count = c1 }),
      OrderedIdxT (k2, v2, { key_count = c2 }) ) ->
      OrderedIdxT
        (unify_exn k1 k2, unify_exn v1 v2, { key_count = I.O.(c1 || c2) })
  | ( HashIdxT (kt1, vt1, { key_count = kc1 }),
      HashIdxT (kt2, vt2, { key_count = kc2 }) ) ->
      let kt = unify_exn kt1 kt2 in
      let vt = unify_exn vt1 vt2 in
      HashIdxT (kt, vt, { key_count = I.O.(kc1 || kc2) })
  | EmptyT, t | t, EmptyT -> t
  | FuncT (t, `Child_sum), FuncT (t', `Child_sum) ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Child_sum)
  | FuncT (t, `Width w), FuncT (t', `Width w') when Int.(w = w') ->
      FuncT (List.map2_exn ~f:unify_exn t t', `Width w)
  | NullT, _
  | IntT _, _
  | DateT _, _
  | FixedT _, _
  | BoolT _, _
  | StringT _, _
  | TupleT _, _
  | ListT _, _
  | HashIdxT _, _
  | OrderedIdxT _, _
  | FuncT _, _ ->
      fail "Unexpected types."

let rec width = function
  | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> 1
  | TupleT (ts, _) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | ListT (t, _) -> width t
  | HashIdxT (kt, vt, _) | OrderedIdxT (kt, vt, _) -> width kt + width vt
  | EmptyT -> 0
  | FuncT (ts, `Child_sum) ->
      List.map ts ~f:width |> List.sum (module Int) ~f:(fun x -> x)
  | FuncT (_, `Width w) -> w

let rec count = function
  | EmptyT -> I.of_int 0
  | NullT | IntT _ | BoolT _ | StringT _ | FixedT _ | DateT _ -> I.of_int 1
  | TupleT (ts, { kind = `Concat }) -> List.sum (module Abs_int) ts ~f:count
  | TupleT (ts, { kind = `Cross }) ->
      List.map ts ~f:count |> List.fold_left ~init:(I.of_int 1) ~f:I.( * )
  | OrderedIdxT (_, vt, { key_count }) -> I.(count vt || (key_count * count vt))
  | HashIdxT (_, vt, _) -> count vt
  | ListT (_, { count }) -> count
  | FuncT _ -> I.top

let hash_kind_of_key_type _ _ = `Direct

let hash_kind_exn = function
  | HashIdxT (kt, _, m) -> hash_kind_of_key_type m.key_count kt
  | _ -> failwith "Unexpected type."

let range_exn = function
  | IntT x -> x.range
  | DateT x -> x.range
  | _ -> failwith "Has no range."

let header_len field_len =
  match I.to_int field_len with
  | Some _ -> I.of_int 0
  | None -> I.byte_width ~nullable:false field_len |> I.of_int

let rec len =
  let open I.O in
  function
  | EmptyT -> I.zero
  | IntT x -> I.byte_width ~nullable:x.nullable x.range |> I.of_int
  | DateT x -> I.byte_width ~nullable:x.nullable x.range |> I.of_int
  | FixedT x -> I.byte_width ~nullable:x.nullable x.value.range |> I.of_int
  | BoolT _ -> I.of_int 1
  | StringT x -> header_len x.nchars + x.nchars
  | TupleT (ts, _) ->
      let body_len = List.sum (module Abs_int) ts ~f:len in
      body_len + header_len body_len
  | ListT (t, x) ->
      let count_len = header_len x.count in
      let body_len = x.count * len t in
      let len_len = header_len body_len in
      count_len + len_len + body_len
  | OrderedIdxT (kt, vt, m) ->
      let values = m.key_count * len vt in
      oi_map_len kt vt m + values
  | HashIdxT (kt, vt, m) ->
      hi_hash_len kt m + hi_map_len kt vt m + (m.key_count * len vt)
  | FuncT (ts, _) -> List.sum (module Abs_int) ts ~f:len
  | NullT as t -> Error.create "Unexpected type." t [%sexp_of: t] |> Error.raise

and oi_map_len kt vt m = I.(m.key_count * (len kt + of_int (oi_ptr_size vt m)))
and oi_ptr_size vt m = I.(byte_width ~nullable:false (m.key_count * len vt))

and hi_hash_len ?(bytes_per_key = I.of_int 1) kt m =
  let open I.O in
  match hash_kind_of_key_type m.key_count kt with
  | `Universal -> I.of_int Int.(8 * 3)
  | `Direct -> I.of_int 0
  | `Cmph ->
      (* The interval represents uncertainty about the hash size, and CMPH hashes
         seem to have some fixed overhead ~100B? *)
      (m.key_count * bytes_per_key * Interval (1, 2)) + I.of_int 128

and hi_map_len kt vt m =
  let open I.O in
  match hash_kind_of_key_type m.key_count kt with
  | `Universal -> I.ceil_pow2 m.key_count
  | `Direct -> range_exn kt * I.of_int (hi_ptr_size kt vt m)
  | `Cmph -> m.key_count * Interval (1, 2) * I.of_int (hi_ptr_size kt vt m)

and hi_ptr_size kt vt m =
  I.(byte_width ~nullable:false (m.key_count * (len kt + len vt)))

(** Returns the least general type of a layout. *)
let least_general_of_layout r =
  let rec f r =
    match r.node with
    | Range _ -> FuncT ([], `Width 1)
    | Select (ps, r') | GroupBy (ps, _, r') ->
        FuncT ([ f r' ], `Width (List.length ps))
    | OrderBy { rel = r'; _ } | Filter (_, r') | Dedup r' ->
        FuncT ([ f r' ], `Child_sum)
    | Join { r1; r2; _ } -> FuncT ([ f r1; f r2 ], `Child_sum)
    | AEmpty -> EmptyT
    | AScalar s -> Pred.to_type s.s_pred |> least_general_of_primtype
    | AList { l_values = r'; _ } -> ListT (f r', { count = Bottom })
    | DepJoin { d_lhs; d_rhs; _ } -> FuncT ([ f d_lhs; f d_rhs ], `Child_sum)
    | AHashIdx { hi_key_layout = Some kr; hi_values = vr; _ } ->
        HashIdxT (f kr, f vr, { key_count = Bottom })
    | AOrderedIdx { oi_key_layout = Some kr; oi_values = vr; _ } ->
        OrderedIdxT (f kr, f vr, { key_count = Bottom })
    | ATuple (rs, k) ->
        let kind =
          match k with
          | Cross -> `Cross
          | Concat -> `Concat
          | _ -> failwith "Unsupported"
        in
        TupleT (List.map rs ~f, { kind })
    | AOrderedIdx { oi_key_layout = None; _ }
    | AHashIdx { hi_key_layout = None; _ } ->
        failwith "Missing key layout."
    | Relation _ -> failwith "Layout is still abstract."
    | _ -> failwith "unsupported"
  in
  f r

(** Returns a layout type that is general enough to hold all of the data. *)
class ['self] type_fold =
  object (_ : 'self)
    inherit [_, _] abslayout_fold
    method! select _ (exprs, _) t = FuncT ([ t ], `Width (List.length exprs))
    method join _ _ t1 t2 = FuncT ([ t1; t2 ], `Child_sum)
    method depjoin _ _ t1 t2 = FuncT ([ t1; t2 ], `Child_sum)
    method! filter _ _ t = FuncT ([ t ], `Child_sum)
    method! order_by _ _ t = FuncT ([ t ], `Child_sum)
    method! dedup _ t = FuncT ([ t ], `Child_sum)

    method! group_by _ (exprs, _, _) t =
      FuncT ([ t ], `Width (List.length exprs))

    method empty _ = EmptyT

    method scalar _ _ =
      function
      | Value.Date x ->
          let x = Date.to_int x in
          DateT { range = I.of_int x; nullable = false }
      | Int x -> IntT { range = I.of_int x; nullable = false }
      | Bool _ -> BoolT { nullable = false }
      | String x ->
          StringT { nchars = I.of_int (String.length x); nullable = false }
      | Null -> NullT
      | Fixed x -> FixedT { value = Abs_fixed.of_fixed x; nullable = false }

    method list _ { l_values = elem_l; _ } =
      let init = (least_general_of_layout elem_l, 0) in
      let fold (t, c) (_, t') = (unify_exn t t', c + 1) in
      let extract (elem_type, num_elems) =
        ListT (elem_type, { count = I.of_int num_elems })
      in
      Fold { init; fold; extract }

    method tuple _ (_, kind) =
      let kind =
        match kind with
        | Cross -> `Cross
        | Zip -> failwith "zip tuples not supported"
        | Concat -> `Concat
      in
      let init = RevList.empty in
      let fold = RevList.( ++ ) in
      let extract ts = TupleT (RevList.to_list ts, { kind }) in
      Fold { init; fold; extract }

    method hash_idx _ h =
      let init =
        ( 0,
          least_general_of_layout (Option.value_exn h.hi_key_layout),
          least_general_of_layout h.hi_values )
      in
      let fold (kct, kt, vt) (_, kt', vt') =
        (kct + 1, unify_exn kt kt', unify_exn vt vt')
      in
      let extract (kct, kt, vt) =
        HashIdxT (kt, vt, { key_count = I.of_int kct })
      in
      Fold { init; fold; extract }

    method ordered_idx _ { oi_values = value_l; oi_key_layout; _ } =
      let key_l = Option.value_exn oi_key_layout in
      let init =
        (least_general_of_layout key_l, least_general_of_layout value_l, 0)
      in
      let fold (kt, vt, ct) (_, kt', vt') =
        (unify_exn kt kt', unify_exn vt vt', ct + 1)
      in
      let extract (kt, vt, ct) =
        OrderedIdxT (kt, vt, { key_count = I.of_int ct })
      in
      Fold { init; fold; extract }
  end

let type_of r =
  info (fun m -> m "Computing type of abstract layout.");
  let type_ = (new type_fold)#run r.meta#fold_stream r in
  info (fun m -> m "The type is: %s" (Sexp.to_string_hum ([%sexp_of: t] type_)));
  type_

let annotate r =
  let rec annot r t =
    r.meta#set_type t;
    match (r.node, t) with
    | ( (AScalar _ | Range _),
        (IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | NullT) ) ->
        ()
    | AList { l_values = r'; _ }, ListT (t', _)
    | (Filter (_, r') | Select (_, r')), FuncT ([ t' ], _) ->
        annot r' t'
    | AHashIdx h, HashIdxT (kt, vt, _) ->
        Option.iter h.hi_key_layout ~f:(fun kr -> annot kr kt);
        annot h.hi_values vt
    | AOrderedIdx { oi_values = vr; oi_key_layout; _ }, OrderedIdxT (kt, vt, _)
      ->
        Option.iter oi_key_layout ~f:(fun kr -> annot kr kt);
        annot vr vt
    | ATuple (rs, _), TupleT (ts, _) -> (
        match List.iter2 rs ts ~f:annot with
        | Ok () -> ()
        | Unequal_lengths ->
            Error.create "Mismatched tuple type." (r, t)
              [%sexp_of: _ Ast.annot * t]
            |> Error.raise)
    | DepJoin { d_lhs; d_rhs; _ }, FuncT ([ t1; t2 ], _) ->
        annot d_lhs t1;
        annot d_rhs t2
    | ( ( Select _ | Filter _ | DepJoin _ | Join _ | GroupBy _ | OrderBy _
        | Dedup _ | Relation _ | AEmpty | AScalar _ | AList _ | ATuple _
        | AHashIdx _ | AOrderedIdx _ | Range _ ),
        ( NullT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | TupleT _
        | ListT _ | HashIdxT _ | OrderedIdxT _ | FuncT _ | EmptyT ) ) ->
        Error.create "Unexpected type." (r, t) [%sexp_of: _ annot * t]
        |> Error.raise
    | _ -> failwith "unsupported"
  in

  let r =
    V.map_meta
      (fun m ->
        object
          val mutable type_ = None
          method fold_stream = m#fold_stream
          method type_ = Option.value_exn type_
          method set_type t = type_ <- Some t
          method eq = m#eq
          method meta = m
        end)
      r
  in
  annot r @@ type_of r;
  let visitor =
    object
      inherit [_] Visitors.runtime_subquery_visitor
      method visit_Subquery r = annot r @@ type_of r
    end
  in
  visitor#visit_t () r;
  V.map_meta
    (fun m ->
      object
        method type_ = m#type_
        method fold_stream = m#fold_stream
        method eq = m#eq
        method meta = m#meta
      end)
    r

module Parallel = struct
  type error = [ `Db_error of Db.Async.error ]

  let pp_err f fmt = function
    | `Db_error e ->
        Fmt.pf fmt "Database error: %a" Error.pp (Db.Async.to_error e)
    | x -> f fmt x

  module Type_builder = struct
    open Option.Let_syntax

    type agg =
      | Simple of (Pred.t * string)
      | Subquery of (Ast.t * (Pred.t * string) list)

    type nonrec t = {
      aggs : agg list;
      build : Value.t Map.M(String).t -> t list -> t;
    }

    let eval ctx (_, n) = Map.find ctx n
    let wrap p = (p, Fresh.name Global.fresh "x%d")
    let func_t t = { aggs = []; build = (fun _ ts -> FuncT (ts, t)) }
    let empty_t = { aggs = []; build = (fun _ _ -> EmptyT) }
    let null_t = { aggs = []; build = (fun _ _ -> NullT) }

    let eval_interval ~to_int ctx lo hi =
      let range =
        let%bind lo = eval ctx lo and hi = eval ctx hi in
        let%map lo = to_int lo and hi = to_int hi in
        I.Interval (lo, hi)
      in
      Option.value range ~default:I.top

    let int_t p =
      let min = wrap @@ `Min p and max = wrap @@ `Max p in
      let build ctx _ =
        let range = eval_interval ~to_int:Value.to_int ctx min max in
        IntT { range; nullable = false (* TODO *) }
      in
      { aggs = [ Simple min; Simple max ]; build }

    let bool_t =
      { aggs = []; build = (fun _ _ -> BoolT { nullable = false (* TODO *) }) }

    let date_t p =
      let min = wrap @@ `Min p and max = wrap @@ `Max p in
      let build ctx _ =
        let range =
          eval_interval
            ~to_int:(fun x -> Option.map ~f:Date.to_int @@ Value.to_date x)
            ctx min max
        in
        DateT { range; nullable = false (* TODO *) }
      in
      { aggs = [ Simple min; Simple max ]; build }

    let fixed_t _ =
      {
        aggs = [];
        build =
          (fun _ _ ->
            FixedT { value = Abs_fixed.top; nullable = false (* TODO *) });
      }

    let string_t _ =
      {
        aggs = [];
        build =
          (fun _ _ -> StringT { nchars = I.top; nullable = false (* TODO *) });
      }

    let scalar_t p =
      match Pred.to_type p with
      | NullT -> null_t
      | IntT _ -> int_t p
      | BoolT _ -> bool_t
      | DateT _ -> date_t p
      | FixedT _ -> fixed_t p
      | StringT _ -> string_t p
      | VoidT | TupleT _ -> failwith "Invalid scalar types."

    let count_t q f =
      let agg_name = Fresh.name Global.fresh "ct%d" in
      let counts = A.group_by [ (`Count, agg_name) ] [] q in
      let count = `Name (Name.create agg_name) in
      let min = wrap @@ `Min count and max = wrap @@ `Max count in
      let build ctx ts =
        let range = eval_interval ~to_int:Value.to_int ctx min max in
        f ts range
      in
      { aggs = [ Subquery (counts, [ min; max ]) ]; build }

    let list_error l =
      Error.create "Unexpected list." l [%sexp_of: t list] |> Error.raise

    let list_t x =
      count_t x.l_keys @@ fun ts count ->
      match ts with [ tv ] -> ListT (tv, { count }) | _ -> list_error ts

    let hash_idx_t x =
      count_t x.hi_keys @@ fun ts key_count ->
      match ts with
      | [ tk; tv ] -> HashIdxT (tk, tv, { key_count })
      | _ -> list_error ts

    let ordered_idx_t x =
      count_t x.oi_keys @@ fun ts key_count ->
      match ts with
      | [ tk; tv ] -> OrderedIdxT (tk, tv, { key_count })
      | _ -> list_error ts

    let tuple_t kind =
      let kind =
        match kind with
        | Cross -> `Cross
        | Concat -> `Concat
        | _ -> failwith "Unexpected tuple kind."
      in
      { aggs = []; build = (fun _ ts -> TupleT (ts, { kind })) }

    let of_ralgebra = function
      | Select (ps, _) | GroupBy (ps, _, _) -> func_t (`Width (List.length ps))
      | Join _ | DepJoin _ | Filter _ | OrderBy _ | Dedup _ -> func_t `Child_sum
      | AEmpty -> empty_t
      | AScalar p -> scalar_t p.s_pred
      | AList x -> list_t x
      | AHashIdx x -> hash_idx_t x
      | AOrderedIdx x -> ordered_idx_t x
      | ATuple (_, kind) -> tuple_t kind
      | Relation _ | Range _ -> null_t
      | _ -> failwith "unsupported"

    let rec annot r =
      {
        node = query r.node;
        meta =
          (let builder = of_ralgebra r.node in
           object
             method builder = builder
           end);
      }

    and query q = V.Map.query annot pred q
    and pred p = V.Map.pred annot pred p

    let type_of ctx r =
      let rec annot r =
        let ts =
          match r.node with
          | Select (_, r)
          | GroupBy (_, _, r)
          | Filter (_, r)
          | OrderBy { rel = r; _ }
          | Dedup r
          | AList { l_values = r; _ } ->
              [ annot r ]
          | Join { r1; r2; _ }
          | DepJoin { d_lhs = r1; d_rhs = r2; _ }
          | AHashIdx { hi_key_layout = Some r1; hi_values = r2; _ }
          | AOrderedIdx { oi_key_layout = Some r1; oi_values = r2; _ } ->
              [ annot r1; annot r2 ]
          | ATuple (rs, _) -> List.map rs ~f:annot
          | AEmpty | AScalar _ | Relation _ | Range _ -> []
          | AOrderedIdx { oi_key_layout = None; _ }
          | AHashIdx { hi_key_layout = None; _ } ->
              failwith "Missing key layout."
          | _ -> failwith "unsupported"
        in

        r.meta#builder.build ctx ts
      in
      try annot r
      with exn ->
        Exn.reraise exn (Fmt.str "type_of failed on:@ %a" Abslayout_pp.pp r)
  end

  let unscope n =
    match Name.scope n with
    | Some s ->
        Name.create
          (sprintf "%d_%s_%d" s (Name.name n) (Fresh.int Global.fresh))
    | None -> n

  module Context = struct
    module T = struct
      type t = Ast.t list [@@deriving compare, sexp]
    end

    include T
    include Comparator.Make (T)

    let to_ralgebra_simple ctxs aggs =
      if List.is_empty aggs then []
      else
        let renaming =
          List.rev ctxs
          |> List.concat_mapi ~f:(fun i r ->
                 Schema.schema r
                 |> List.map ~f:(fun n ->
                        let n = Name.set_index n i in
                        (n, unscope n)))
        in
        let select_list =
          List.map renaming ~f:(fun (n, n') -> (`Name n, Name.name n'))
        in
        let bindings =
          let one = A.scalar (`Int 0) (Fresh.name Global.fresh "x%d") in
          if List.is_empty select_list then one
          else
            let select = A.select select_list one in
            let rec to_dep_join = function
              | [] -> select
              | r :: ctxs -> A.dep_join r (to_dep_join ctxs)
            in
            to_dep_join ctxs
        in
        let aggs =
          let subst =
            List.map renaming ~f:(fun (n, n') -> (n, `Name n'))
            |> Map.of_alist_exn (module Name)
          in
          List.map aggs ~f:(fun (p, n) -> (Pred.subst subst p, n))
        in
        [ A.group_by aggs [] bindings ]

    let to_ralgebra_subquery ctxs (subquery, aggs) =
      let rec to_dep_join = function
        | [] -> subquery
        | r :: ctxs -> A.dep_join r (to_dep_join ctxs)
      in
      A.group_by aggs [] (to_dep_join ctxs)

    let drop_orderby r =
      let rec annot r =
        match r.node with
        | OrderBy { rel; _ } -> annot rel
        | _ -> V.Map.annot query r
      and query q = V.Map.query annot pred q
      and pred p = V.Map.pred annot pred p in
      annot r

    (** Convert a context to a query that selects all bindings in the context.
       We don't care about the order that these bindings appear in. *)
    let to_ralgebra ctxs builders =
      let simple, subquery =
        List.map builders ~f:(fun b -> b.Type_builder.aggs)
        |> List.concat
        |> List.partition_map ~f:(function
             | Type_builder.Simple ps -> First ps
             | Subquery p -> Second p)
      in
      to_ralgebra_simple ctxs simple
      @ List.map subquery ~f:(to_ralgebra_subquery ctxs)
      |> List.map ~f:drop_orderby
  end

  let contexts r =
    let empty q = [ ([], q) ] in
    let wrap r = List.map ~f:(fun (ctxs, q) -> (strip_meta r :: ctxs, q)) in

    let plus = ( @ ) and zero = [] in
    let rec annot r =
      let this_ctx = empty r.meta in
      let child_ctxs =
        match r.node with
        | AList { l_keys = qk; l_values = qv } -> wrap qk (annot qv)
        | AOrderedIdx { oi_keys = qk; oi_values = qv; oi_key_layout; _ } ->
            let wrap = wrap qk in
            let qk = Option.value_exn oi_key_layout in
            plus (wrap (annot qv)) (wrap (annot qk))
        | AHashIdx h ->
            let wrap = wrap h.hi_keys in
            let qk = Option.value_exn h.hi_key_layout in
            let qv = h.hi_values in
            plus (wrap (annot qv)) (wrap (annot qk))
        | q -> V.Reduce.query zero plus annot pred q
      in
      plus this_ctx child_ctxs
    and pred _ = zero in

    annot r
    |> List.map ~f:(fun (k, v) -> (k, [ v ]))
    |> Map.of_alist_reduce (module Context) ~f:( @ )

  let%expect_test "" =
    let ctxs =
      contexts
        (Abslayout.of_string_exn "alist(r, alist(s, ascalar((1.x + 0.x) as v)))")
    in
    print_s [%message (ctxs : _ list Map.M(Context).t)];
    [%expect
      {|
      (ctxs
       ((() (_))
        ((((node (Relation ((r_name r) (r_schema ())))) (meta <opaque>))) (_))
        ((((node (Relation ((r_name r) (r_schema ())))) (meta <opaque>))
          ((node (Relation ((r_name s) (r_schema ())))) (meta <opaque>)))
         (_)))) |}]

  let type_of ?timeout conn r =
    let open Result.Let_syntax in
    let r = (r :> < > annot) in
    let r = Type_builder.annot r in
    let%bind queries =
      contexts r |> Map.to_alist
      |> List.concat_map ~f:(fun (ctx, builders) ->
             let builders = List.map ~f:(fun b -> b#builder) builders in
             Context.to_ralgebra ctx builders)
      |> List.map ~f:(fun r ->
             debug (fun m -> m "Pre-opt:@ %a" Abslayout_pp.pp r);
             let%map r =
               (* Simplify_tactic.simplify conn *)
               r |> Resolve.resolve ~params:(Set.empty (module Name))
             in
             debug (fun m -> m "Post-opt:@ %a" Abslayout_pp.pp r);
             r)
      |> Result.all
    in

    let results =
      let open Lwt in
      Lwt_list.map_p
        (fun r ->
          let start_time = Time.now () in
          let%lwt tup = Lwt_stream.get @@ Db.Async.exec ?timeout conn r in
          let end_time = Time.now () in
          let total_time = Time.diff end_time start_time in
          match tup with
          | Some (Ok t) ->
              info (fun m ->
                  m "Query succeeded after %a." Time.Span.pp total_time);
              return
                (List.hd_exn t
                |> List.map2_exn (Schema.names r) ~f:(fun n v -> (n, v))
                |> Option.return |> Result.return)
          | Some (Error { info = `Timeout; _ }) ->
              info (fun m ->
                  m "Query timed out after %a." Time.Span.pp total_time);
              return @@ Result.return None
          | Some (Error e) -> return @@ Result.fail (`Db_error e)
          | None -> failwith "BUG: expected a tuple.")
        queries
      |> Lwt_main.run
    in

    Result.(
      all results >>| fun results ->
      let ctx =
        List.filter_map results ~f:Fun.id
        |> List.concat
        |> Map.of_alist_exn (module String)
      in
      Type_builder.type_of ctx r)
end
