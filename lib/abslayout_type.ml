open! Core
open Collections
open Ast
open Abslayout
open Abslayout_fold
module T = Type

let drop_meta q = Abslayout_visitors.map_meta (fun _ -> ()) q

(** Returns the least general type of a layout. *)
let rec least_general_of_layout r =
  match r.node with
  | Range _ -> T.FuncT ([], `Width 1)
  | Select (ps, r') | GroupBy (ps, _, r') ->
      T.FuncT ([ least_general_of_layout r' ], `Width (List.length ps))
  | OrderBy { rel = r'; _ } | Filter (_, r') | Dedup r' ->
      FuncT ([ least_general_of_layout r' ], `Child_sum)
  | Join { r1; r2; _ } ->
      FuncT
        ([ least_general_of_layout r1; least_general_of_layout r2 ], `Child_sum)
  | AEmpty -> EmptyT
  | AScalar p -> Pred.to_type p |> T.least_general_of_primtype
  | AList (_, r') -> ListT (least_general_of_layout r', { count = Bottom })
  | DepJoin { d_lhs; d_rhs; _ } ->
      FuncT
        ( [ least_general_of_layout d_lhs; least_general_of_layout d_rhs ],
          `Child_sum )
  | AHashIdx h ->
      HashIdxT
        ( least_general_of_layout (h_key_layout h),
          least_general_of_layout h.hi_values,
          { key_count = Bottom } )
  | AOrderedIdx (_, vr, { oi_key_layout = Some kr; _ }) ->
      OrderedIdxT
        ( least_general_of_layout kr,
          least_general_of_layout vr,
          { key_count = Bottom } )
  | ATuple (rs, k) ->
      let kind =
        match k with
        | Cross -> `Cross
        | Concat -> `Concat
        | _ -> failwith "Unsupported"
      in
      TupleT (List.map rs ~f:least_general_of_layout, { kind })
  | As (_, r') -> least_general_of_layout r'
  | AOrderedIdx (_, _, { oi_key_layout = None; _ }) | Relation _ ->
      failwith "Layout is still abstract."

let least_general_of_layout r = least_general_of_layout @@ drop_meta r

(** Returns a layout type that is general enough to hold all of the data. *)
class ['self] type_fold =
  object (_ : 'self)
    inherit [_] abslayout_fold

    method! select _ (exprs, _) t = T.FuncT ([ t ], `Width (List.length exprs))

    method join _ _ t1 t2 = T.FuncT ([ t1; t2 ], `Child_sum)

    method depjoin _ _ t1 t2 = T.FuncT ([ t1; t2 ], `Child_sum)

    method! filter _ _ t = T.FuncT ([ t ], `Child_sum)

    method! order_by _ _ t = T.FuncT ([ t ], `Child_sum)

    method! dedup _ t = T.FuncT ([ t ], `Child_sum)

    method! group_by _ (exprs, _, _) t =
      FuncT ([ t ], `Width (List.length exprs))

    method empty _ = EmptyT

    method scalar _ _ =
      function
      | Value.Date x ->
          let x = Date.to_int x in
          DateT
            {
              range = T.AbsInt.of_int x;
              nullable = false;
              distinct = T.Distinct.singleton (module Int) x 1;
            }
      | Int x ->
          IntT
            {
              range = T.AbsInt.of_int x;
              nullable = false;
              distinct = T.Distinct.singleton (module Int) x 1;
            }
      | Bool _ -> BoolT { nullable = false }
      | String x ->
          StringT
            {
              nchars = T.AbsInt.of_int (String.length x);
              nullable = false;
              distinct = T.Distinct.singleton (module String) x 1;
            }
      | Null -> NullT
      | Fixed x -> FixedT { value = T.AbsFixed.of_fixed x; nullable = false }

    method list _ ((_, elem_l) as l) =
      let init = (least_general_of_layout elem_l, 0) in
      let fold (t, c) (_, t') =
        try (T.unify_exn t t', c + 1)
        with Type.TypeError _ as exn ->
          Logs.err (fun m -> m "Type checking failed on: %a" pp (list' l));
          Logs.err (fun m ->
              m "%a does not unify with %a" Sexp.pp_hum
                ([%sexp_of: T.t] t)
                Sexp.pp_hum
                ([%sexp_of: T.t] t'));
          raise exn
      in
      let extract (elem_type, num_elems) =
        T.ListT (elem_type, { count = T.AbsInt.of_int num_elems })
      in
      Fold { init; fold; extract }

    method tuple _ (_, kind) =
      let kind =
        match kind with
        | Cross -> `Cross
        | Zip -> failwith ""
        | Concat -> `Concat
      in
      let init = RevList.empty in
      let fold = RevList.( ++ ) in
      let extract ts = T.TupleT (RevList.to_list ts, { kind }) in
      Fold { init; fold; extract }

    method hash_idx _ h =
      let init =
        ( 0,
          least_general_of_layout (h_key_layout h),
          least_general_of_layout h.hi_values )
      in
      let fold (kct, kt, vt) (_, kt', vt') =
        (kct + 1, T.unify_exn kt kt', T.unify_exn vt vt')
      in
      let extract (kct, kt, vt) =
        T.HashIdxT (kt, vt, { key_count = T.AbsInt.of_int kct })
      in
      Fold { init; fold; extract }

    method ordered_idx _ (_, value_l, { oi_key_layout; _ }) =
      let key_l = Option.value_exn oi_key_layout in
      let init =
        (least_general_of_layout key_l, least_general_of_layout value_l, 0)
      in
      let fold (kt, vt, ct) (_, kt', vt') =
        (T.unify_exn kt kt', T.unify_exn vt vt', ct + 1)
      in
      let extract (kt, vt, ct) =
        T.OrderedIdxT (kt, vt, { key_count = T.AbsInt.of_int ct })
      in
      Fold { init; fold; extract }
  end

let type_of ?timeout conn r =
  Log.info (fun m -> m "Computing type of abstract layout.");
  let type_ = (new type_fold)#run ?timeout conn r in
  Log.info (fun m ->
      m "The type is: %s" (Sexp.to_string_hum ([%sexp_of: Type.t] type_)));
  type_

let annotate_type conn r =
  let rec annot r t =
    let open Type in
    Meta.(set_m r type_ t);
    match (r.node, t) with
    | ( (AScalar _ | Range _),
        (IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | NullT) ) ->
        ()
    | AList (_, r'), ListT (t', _)
    | (Filter (_, r') | Select (_, r')), FuncT ([ t' ], _) ->
        annot r' t'
    | AHashIdx h, HashIdxT (kt, vt, _) ->
        Option.iter h.hi_key_layout ~f:(fun kr -> annot kr kt);
        annot h.hi_values vt
    | AOrderedIdx (_, vr, m), OrderedIdxT (kt, vt, _) ->
        Option.iter m.oi_key_layout ~f:(fun kr -> annot kr kt);
        annot vr vt
    | ATuple (rs, _), TupleT (ts, _) -> (
        match List.iter2 rs ts ~f:annot with
        | Ok () -> ()
        | Unequal_lengths ->
            Error.create "Mismatched tuple type." (r, t) [%sexp_of: Ast.t * T.t]
            |> Error.raise )
    | DepJoin { d_lhs; d_rhs; _ }, FuncT ([ t1; t2 ], _) ->
        annot d_lhs t1;
        annot d_rhs t2
    | As (_, r), _ -> annot r t
    | ( ( Select _ | Filter _ | DepJoin _ | Join _ | GroupBy _ | OrderBy _
        | Dedup _ | Relation _ | AEmpty | AScalar _ | AList _ | ATuple _
        | AHashIdx _ | AOrderedIdx _ | Range _ ),
        ( NullT | IntT _ | DateT _ | FixedT _ | BoolT _ | StringT _ | TupleT _
        | ListT _ | HashIdxT _ | OrderedIdxT _ | FuncT _ | EmptyT ) ) ->
        Error.create "Unexpected type." (r, t) [%sexp_of: Ast.t * t]
        |> Error.raise
  in
  annot r (type_of conn r);
  let visitor =
    object
      inherit Abslayout_visitors.runtime_subquery_visitor

      method visit_Subquery r = annot r (type_of conn r)
    end
  in
  visitor#visit_t () r
