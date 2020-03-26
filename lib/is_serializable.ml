open Ast
module V = Visitors
module A = Abslayout

class ['a] stage_iter =
  object (self : 'a)
    inherit [_] V.iter

    method! visit_AList (ctx, phase) (rk, rv) =
      self#visit_t (ctx, `Compile) rk;
      self#visit_t (ctx, phase) rv

    method! visit_AHashIdx (ctx, phase) h =
      List.iter h.hi_lookup ~f:(self#visit_pred (ctx, phase));
      self#visit_t (ctx, `Compile) h.hi_keys;
      self#visit_t (ctx, phase) h.hi_values

    method! visit_AOrderedIdx (ctx, phase) (rk, rv, m) =
      let bound_iter =
        Option.iter ~f:(fun (p, _) -> self#visit_pred (ctx, phase) p)
      in
      List.iter m.oi_lookup ~f:(fun (b1, b2) ->
          bound_iter b1;
          bound_iter b2);
      self#visit_t (ctx, `Compile) rk;
      self#visit_t (ctx, phase) rv

    method! visit_AScalar (ctx, _) p = self#visit_pred (ctx, `Compile) p
  end

let stage ?(params = Set.empty (module Name)) r =
  let compile_scopes, run_scopes =
    let empty = Set.empty (module String)
    and single = Set.singleton (module String) in
    let zero = (empty, empty)
    and plus (c, r) (c', r') = (Set.union c c', Set.union r r') in
    let rec annot stage r = V.Stage_reduce.annot zero plus query meta stage r
    and query stage q =
      let this =
        match q with
        | AHashIdx { hi_scope; _ } -> (single hi_scope, empty)
        | AOrderedIdx (r, _, _) | AList (r, _) -> (single (A.scope_exn r), empty)
        | DepJoin { d_alias; _ } -> (
            match stage with
            | `Compile -> (single d_alias, empty)
            | `Run -> (empty, single d_alias) )
        | _ -> zero
      and rest = V.Stage_reduce.query zero plus annot pred stage q in
      plus this rest
    and pred stage p = V.Stage_reduce.pred zero plus annot pred stage p
    and meta _ _ = zero in
    annot `Run r
  in
  fun n ->
    match Name.rel n with
    | Some s ->
        if Set.mem compile_scopes s then `Compile
        else if Set.mem run_scopes s then `Run
        else failwith (sprintf "Scope not found: %s" s)
    | None -> if Set.mem params n then `Run else `No_scope

let annotate_stage r =
  let stage = stage r in
  let rec annot r =
    let node = query r.node
    and meta =
      object
        method meta = r.meta

        method stage = stage
      end
    in
    { node; meta }
  and query q = V.Map.query annot pred q
  and pred p = V.Map.pred annot pred p in
  annot r

let is_static ?(params = Set.empty (module Name)) r =
  Free.free r
  |> Set.for_all ~f:(fun n ->
         match r.meta#stage n with
         | `Compile -> true
         | `No_scope -> not (Set.mem params n)
         | `Run -> false)

exception Un_serial of string

let ops_serializable_exn r =
  let visitor =
    object
      inherit [_] stage_iter as super

      method! visit_t ((), s) r =
        super#visit_t ((), s) r;
        match (s, r.node) with
        | `Run, (Relation _ | GroupBy (_, _, _) | Join _ | OrderBy _ | Dedup _)
          ->
            raise
            @@ Un_serial
                 (Format.asprintf
                    "Cannot serialize: Bad operator in run-time position %a"
                    A.pp r)
        | _ -> ()
    end
  in
  visitor#visit_t ((), `Run) r

let names_serializable_exn ?params p r =
  let stage = stage ?params r in
  let visitor =
    object
      inherit [_] stage_iter as super

      method! visit_Name (_, s) n =
        match (stage n, s) with
        | `Compile, `Run | `Run, `Compile ->
            let stage = match s with `Compile -> "compile" | `Run -> "run" in
            let msg =
              Fmt.str "Cannot serialize: Found %a in %s time position." Name.pp
                n stage
            in
            raise @@ Un_serial msg
        | _ -> ()

      method! visit_t (_, s) = super#visit_t ((), s)
    end
  in
  visitor#visit_t ((), `Run) @@ Path.get_exn p r

(** Return true if `r` is serializable. This function performs two checks:
    - `r` must not contain any compile time only operations in run time position.
    - Run-time names may only appear in run-time position and vice versa. *)
let is_serializeable ?(path = Path.root) ?params r =
  let r' = Path.get_exn path r in
  try
    ops_serializable_exn r';
    names_serializable_exn ?params path r;
    Ok ()
  with Un_serial msg -> Error msg
