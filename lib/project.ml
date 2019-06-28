open! Core
open Abslayout
open Collections

let project_def refcnt p =
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
        true )

let project_defs refcnt ps = List.filter ps ~f:(project_def refcnt)

(** True if all fields emitted by r are unreferenced when emitted by r'. *)
let all_unref_at r r' =
  let refcnt = Meta.find_exn r' Meta.refcnt in
  let schema = schema_exn r in
  List.for_all (Schema.unscoped schema) ~f:(fun n ->
      match Map.(find refcnt n) with Some c -> c = 0 | None -> false)

(** True if all fields emitted by r are unreferenced. *)
let all_unref r = all_unref_at r r

let _pp_with_refcount, _ =
  mk_pp
    ~pp_meta:(fun fmt meta ->
      let open Format in
      match Univ_map.find meta Meta.refcnt with
      | Some r ->
          fprintf fmt "@[<hv 2>{" ;
          Map.iteri r ~f:(fun ~key:n ~data:c ->
              if c > 0 then fprintf fmt "%a=%d,@ " Name.pp n c) ;
          fprintf fmt "}@]"
      | None -> ())
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
          | AHashIdx _ | ATuple _ -> Exact
          | _ -> c
        in
        Meta.set (super#visit_t c r) count c

      method! visit_AHashIdx c h =
        AHashIdx
          {(self#visit_hash_idx c h) with hi_keys= self#visit_t AtLeastOne h.hi_keys}

      method! visit_AOrderedIdx c (rk, rv, m) =
        AOrderedIdx
          (self#visit_t AtLeastOne rk, self#visit_t c rv, self#visit_ordered_idx c m)
    end
  in
  visitor#visit_t Exact r

(* This is just a sentinal so we can use any value. *)
let dummy = Bool false

let get_refcnt r =
  Option.value_exn
    ~error:(Error.createf "No refcnt found %a" pp_small_str r)
    (Meta.find r Meta.refcnt)

let project_visitor =
  object (self : 'a)
    inherit [_] map as super

    method! visit_t () r =
      let refcnt = get_refcnt r in
      let count = Meta.find_exn r count in
      if all_unref r && count = AtLeastOne then scalar dummy
      else
        match r.node with
        | Select (ps, r) -> select (project_defs refcnt ps) (self#visit_t () r)
        | Dedup r -> dedup (self#visit_t () r)
        | GroupBy (ps, ns, r) ->
            group_by (project_defs refcnt ps) ns (self#visit_t () r)
        | AList (rk, rv) ->
            let scope = scope_exn rk in
            let rk = strip_scope rk in
            let rk =
              let refcnt = get_refcnt rk in
              let schema = schema_exn rk in
              let old_n = List.length schema in
              let ps =
                project_defs refcnt (schema_exn rk |> List.map ~f:(fun n -> Name n))
              in
              let new_n = List.length ps in
              if old_n > new_n then select ps rk else self#visit_t () rk
            in
            list rk scope (self#visit_t () rv)
        | AScalar p -> if project_def refcnt p then scalar p else scalar dummy
        | ATuple ([], _) -> empty
        | ATuple ([r], _) -> self#visit_t () r
        | ATuple (rs, Concat) -> tuple (List.map rs ~f:(self#visit_t ())) Concat
        | ATuple (rs, Cross) ->
            let rs =
              (* Remove unreferenced parts of the tuple. *)
              List.filter rs ~f:(fun r ->
                  let is_unref = all_unref r in
                  let is_scalar =
                    match r.node with AScalar _ -> true | _ -> false
                  in
                  let should_remove =
                    is_unref && is_scalar
                    (* match count with
                     * (\* If the count matters, then we can only remove
                     *        unreferenced scalars. *\)
                     * | Exact -> is_unref && is_scalar
                     * (\* Otherwise we can remove anything unreferenced. *\)
                     * | AtLeastOne -> is_unref *)
                  in
                  not should_remove)
              |> List.map ~f:(self#visit_t ())
            in
            let rs = if List.length rs = 0 then [scalar dummy] else rs in
            tuple rs Cross
        | Join {r1; r2; pred} -> (
          match count with
          | Exact -> join pred (self#visit_t () r1) (self#visit_t () r2)
          (* If one side of a join is unused then the join can be dropped. *)
          | AtLeastOne ->
              if all_unref_at r1 r then self#visit_t () r2
              else if all_unref_at r2 r then self#visit_t () r1
              else join pred (self#visit_t () r1) (self#visit_t () r2) )
        | DepJoin {d_lhs; d_rhs; d_alias} -> (
          match count with
          | Exact ->
              dep_join (self#visit_t () d_lhs) d_alias (self#visit_t () d_rhs)
          (* If one side of a join is unused then the join can be dropped. *)
          | AtLeastOne ->
              if all_unref d_lhs then self#visit_t () d_rhs
              else if all_unref d_rhs then scalar dummy
              else dep_join (self#visit_t () d_lhs) d_alias (self#visit_t () d_rhs)
          )
        | _ -> super#visit_t () r
  end

let project_once r =
  let r = annotate_count r in
  let r = project_visitor#visit_t () r in
  r

let project ?(params = Set.empty (module Name)) r =
  let rec loop r =
    let r' = Resolve.resolve r ~params |> project_once in
    if Abslayout.O.(r = r') then r' else loop r'
  in
  loop r
