open Base
open Printf
open Collections
module A = Abslayout

module Config = struct
  module type S = sig
    val check_transforms : bool

    val params : Set.M(Name.Compare_no_type).t
  end
end

module Make (Config : Config.S) (M : Abslayout_db.S) () = struct
  type t = {name: string; f: A.t -> A.t list} [@@deriving sexp]

  let fresh = Fresh.create ()

  let run_everywhere ?(stage = `Both) {name; f= f_inner} =
    let cstage ls = match stage with `Both | `Compile -> ls | `Run -> [] in
    let rstage ls = match stage with `Both | `Run -> ls | `Compile -> [] in
    let rec p = function
      | A.Exists r -> List.map ~f:(fun r -> A.Exists r) (f r)
      | A.First r -> List.map ~f:(fun r -> A.First r) (f r)
      | ( A.Name _ | A.Int _ | A.Fixed _ | A.Date _ | A.Bool _ | A.String _ | A.Null
        | A.Count | A.Sum _ | A.Avg _ | A.Min _ | A.Max _ ) as pred ->
          [pred]
      | A.Binop (op, r1, r2) ->
          List.map (p r1) ~f:(fun r1' -> A.Binop (op, r1', r2))
          @ List.map (p r2) ~f:(fun r2' -> A.Binop (op, r1, r2'))
      | A.If (r1, r2, r3) ->
          List.map (p r1) ~f:(fun r1' -> A.If (r1', r2, r3))
          @ List.map (p r2) ~f:(fun r2' -> A.If (r1, r2', r3))
          @ List.map (p r3) ~f:(fun r3' -> A.If (r1, r2, r3'))
      | A.Unop (op, r) -> List.map (p r) ~f:(fun r' -> A.Unop (op, r'))
      | A.As_pred (r, n) -> List.map (p r) ~f:(fun r' -> A.As_pred (r', n))
      | A.Substring (r1, r2, r3) ->
          List.map (p r1) ~f:(fun r1' -> A.Substring (r1', r2, r3))
          @ List.map (p r2) ~f:(fun r2' -> A.Substring (r1, r2', r3))
          @ List.map (p r3) ~f:(fun r3' -> A.Substring (r1, r2, r3'))
    and f r =
      let rs = f_inner r in
      let rs' =
        match r.node with
        | A.Scan _ | AEmpty | AScalar _ -> []
        | Dedup r -> List.map ~f:A.dedup (f r)
        | As (n, r) -> List.map ~f:(A.as_ n) (f r)
        | OrderBy {key; order; rel} -> List.map (f rel) ~f:(A.order_by key order)
        | AList (r1, r2) ->
            cstage (List.map (f r1) ~f:(fun r1 -> A.list r1 r2))
            @ rstage (List.map (f r2) ~f:(fun r2 -> A.list r1 r2))
        | Select (fs, r') -> List.map (f r') ~f:(A.select fs)
        | Filter (ps, r') ->
            List.map (p ps) ~f:(fun ps' -> A.filter ps' r')
            @ List.map (f r') ~f:(A.filter ps)
        | Join {r1; r2; pred} ->
            List.map (f r1) ~f:(fun r1 -> A.join pred r1 r2)
            @ List.map (f r2) ~f:(fun r2 -> A.join pred r1 r2)
        | GroupBy (x, y, r') -> List.map (f r') ~f:(A.group_by x y)
        | AHashIdx (r1, r2, m) ->
            cstage (List.map (f r1) ~f:(fun r1 -> A.hash_idx' r1 r2 m))
            @ rstage (List.map (f r2) ~f:(fun r2 -> A.hash_idx' r1 r2 m))
        | AOrderedIdx (r1, r2, m) ->
            cstage (List.map (f r1) ~f:(fun r1 -> A.ordered_idx r1 r2 m))
            @ rstage (List.map (f r2) ~f:(fun r2 -> A.ordered_idx r1 r2 m))
        | ATuple (rs, m) ->
            List.mapi rs ~f:(fun i r ->
                List.map (f r) ~f:(fun r' ->
                    A.tuple (List.take rs i @ [r'] @ List.drop rs (i + 1)) m ) )
            |> List.concat
      in
      rs @ rs'
    in
    {name; f}

  let run_unchecked t r =
    let rs =
      t.f r
      |> List.dedup_and_sort ~compare:[%compare: Abslayout.t]
      |> List.filter ~f:(fun r' -> not ([%compare.equal: Abslayout.t] r r'))
    in
    let len = List.length rs in
    if len > 0 then
      Logs.info (fun m -> m "%d new candidates from running %s." len t.name) ;
    rs

  let run_checked t r =
    let rs = run_unchecked t r in
    let check_schema r' =
      let s = M.to_schema r |> Set.of_list (module Name.Compare_no_type) in
      let s' = M.to_schema r' |> Set.of_list (module Name.Compare_no_type) in
      let schemas_ok = Set.is_subset s ~of_:s' in
      if not schemas_ok then
        Logs.warn (fun m ->
            m "Transform %s not equivalent. Schemas differ: %a %a" t.name
              Sexp.pp_hum
              ([%sexp_of: Set.M(Name).t] s)
              Sexp.pp_hum
              ([%sexp_of: Set.M(Name).t] s') ) ;
      schemas_ok
    in
    let checks = [check_schema] in
    List.map rs ~f:(fun r' ->
        List.for_all checks ~f:(fun c -> c r') |> ignore ;
        A.validate r' ;
        r' )

  let run = if Config.check_transforms then run_checked else run_unchecked

  let no_params r = Set.is_empty (Set.inter (A.names r) Config.params)

  let tf_row_store _ =
    let open A in
    { name= "row-store"
    ; f=
        (fun r ->
          if no_params r then
            let s = M.to_schema r in
            let scalars = List.map s ~f:(fun n -> scalar (Name n)) in
            [list r (tuple scalars Cross)]
          else [] ) }
    |> run_everywhere ~stage:`Run

  let tf_elim_groupby _ =
    let open A in
    { name= "elim-groupby"
    ; f=
        (function
        | {node= GroupBy (ps, key, r); _} as rr when no_params rr ->
            let key_name = Fresh.name fresh "k%d" in
            let key_preds = List.map key ~f:(fun n -> Name n) in
            let filter_pred =
              List.map key ~f:(fun n ->
                  Binop (Eq, Name n, Name {n with relation= Some key_name}) )
              |> List.fold_left ~init:(Bool true) ~f:(fun acc p ->
                     Binop (And, acc, p) )
            in
            let key_layouts = List.map key_preds ~f:scalar in
            [ list
                (as_ key_name (dedup (select key_preds r)))
                (select ps (tuple (key_layouts @ [filter filter_pred r]) Cross)) ]
        | _ -> []) }
    |> run_everywhere

  let tf_elim_groupby_filter _ =
    let open A in
    { name= "elim-groupby-filter"
    ; f=
        (function
        | {node= GroupBy (ps, key, {node= Filter (p, r); _}); _} when no_params r ->
            let key_name = Fresh.name fresh "k%d" in
            let new_key =
              List.map key ~f:(fun n -> sprintf "%s_%s" key_name (Name.to_var n))
            in
            let ps =
              let ctx =
                List.map2_exn key new_key ~f:(fun n n' -> (n, Name (Name.create n')))
                |> Map.of_alist_exn (module Name.Compare_no_type)
              in
              List.map ps ~f:(subst_pred ctx)
            in
            let select_list =
              List.map2_exn key new_key ~f:(fun n n' -> As_pred (Name n, n'))
            in
            let filter_pred =
              List.map2_exn key new_key ~f:(fun n n' ->
                  Binop (Eq, Name n, Name (Name.create n')) )
              |> List.fold_left1 ~f:(fun acc p -> Binop (And, acc, p))
            in
            let key_layouts =
              List.map new_key ~f:(fun n -> scalar (Name (Name.create n)))
            in
            [ list
                (dedup (select select_list r))
                (select ps
                   (tuple (key_layouts @ [filter p (filter filter_pred r)]) Cross))
            ]
        | _ -> []) }
    |> run_everywhere

  let tf_elim_eq_filter _ =
    let open A in
    { name= "elim-eq-filter"
    ; f=
        (function
        | {node= Filter (Binop (Eq, p1, p2), r); _} ->
            List.map [(p1, p2); (p2, p1)] ~f:(fun (p, p') ->
                let k = Fresh.name fresh "k%d" in
                let select_list = [As_pred (p, k)] in
                let filter_pred = Binop (Eq, Name (Name.create k), p) in
                hash_idx (dedup (select select_list r)) (filter filter_pred r) [p']
            )
        | _ -> []) }
    |> run_everywhere

  let tf_elim_cmp_filter _ =
    let open A in
    let gen ?lb ?ub p r =
      let lb = Option.value lb ~default:(Int (Int.min_value + 1)) in
      let ub = Option.value ub ~default:(Int Int.max_value) in
      let k = Fresh.name fresh "k%d" in
      let select_list = [As_pred (p, k)] in
      let filter_pred = Binop (Eq, Name (Name.create k), p) in
      [ ordered_idx
          (dedup (select select_list r))
          (filter filter_pred r)
          {oi_key_layout= None; lookup_low= lb; lookup_high= ub; order= `Desc} ]
    in
    { name= "elim-cmp-filter"
    ; f=
        (function
        | {node= Filter (Binop (And, Binop (Ge, p, lb), Binop (Lt, p', ub)), r); _}
          when [%compare.equal: pred] p p' ->
            gen ~lb ~ub p r
        | {node= Filter (Binop (Le, p, ub), r); _} -> gen ~ub p r
        | _ -> []) }
    |> run_everywhere

  let same_orders r1 r2 =
    let open A in
    let r1 = M.annotate_schema r1 in
    let r2 = M.annotate_schema r2 in
    annotate_eq r1 ;
    annotate_orders r1 ;
    annotate_eq r2 ;
    annotate_orders r2 ;
    [%compare.equal: pred list] Meta.(find_exn r1 order) Meta.(find_exn r2 order)

  let orderby_list key order r1 r2 =
    let open A in
    let r1 = M.annotate_schema r1 in
    let r2 = M.annotate_schema r2 in
    annotate_eq r1 ;
    annotate_eq r2 ;
    let schema1 = Meta.(find_exn r1 schema) in
    let open Core in
    let eqs = Meta.(find_exn r2 eq) in
    let names =
      List.concat_map eqs ~f:(fun (n, n') -> [n; n'])
      @ List.filter_map ~f:pred_to_name key
      @ schema1
    in
    (* Create map from names to sets of equal names. *)
    let eq_map =
      names
      |> List.dedup_and_sort ~compare:[%compare: Name.Compare_no_type.t]
      |> List.map ~f:(fun n -> (n, Union_find.create n))
      |> Hashtbl.of_alist_exn (module Name.Compare_no_type)
    in
    (* Add known equalities. *)
    List.iter eqs ~f:(fun (n, n') ->
        let s = Hashtbl.find_exn eq_map n in
        let s' = Hashtbl.find_exn eq_map n' in
        Union_find.union s s' ) ;
    Logs.debug (fun m -> m "%a" Sexp.pp_hum ([%sexp_of: A.t * A.t] (r1, r2))) ;
    Logs.debug (fun m ->
        m "Known equalities %a" Sexp.pp_hum ([%sexp_of: (Name.t * Name.t) list] eqs)
    ) ;
    let exception No_key in
    try
      let new_key =
        List.map key ~f:(fun p ->
            match pred_to_name p with
            | Some n -> (
                let s = Hashtbl.find_exn eq_map n in
                (* Find an equivalent name in schema 1. *)
                let n' =
                  List.find schema1 ~f:(fun n' ->
                      let s' = Hashtbl.find_exn eq_map n' in
                      Union_find.same_class s s' )
                in
                match n' with Some n' -> Name n' | None -> raise No_key )
            | None -> raise No_key )
      in
      [list (order_by new_key order r1) r2]
    with No_key -> []

  let orderby_cross_tuple key order rs =
    let open A in
    let rs = List.map rs ~f:M.annotate_schema in
    match rs with
    | r :: rs ->
        let schema = Meta.(find_exn r schema) in
        let sschema = Set.of_list (module Name.Compare_no_type) schema in
        let skey =
          Set.of_list
            (module Name.Compare_no_type)
            (List.filter_map ~f:pred_to_name key)
        in
        if Set.is_subset skey ~of_:sschema then
          [tuple (order_by key order r :: rs) Cross]
        else []
    | _ -> []

  let tf_push_orderby _ =
    let open A in
    { name= "push-orderby"
    ; f=
        (fun r ->
          let rs =
            match r with
            | {node= OrderBy {key; rel= {node= Select (ps, r); _}; order}; _} ->
                [select ps (order_by key order r)]
            | {node= OrderBy {key; rel= {node= Filter (ps, r); _}; order}; _} ->
                [filter ps (order_by key order r)]
            | {node= OrderBy {key; rel= {node= AList (r1, r2); _}; order}; _} ->
                (* If we order a lists keys then the keys will be ordered in the
                   list. *)
                orderby_list key order r1 r2
            | {node= OrderBy {key; rel= {node= ATuple (rs, Cross); _}; order}; _} ->
                orderby_cross_tuple key order rs
            | _ -> []
          in
          List.filter rs ~f:(same_orders r) ) }
    |> run_everywhere

  let tf_push_select _ =
    (* Generate aggregates for collections that act by concatenating their children. *)
    let gen_concat_select_list outer_preds inner_rel =
      let open Abslayout in
      let visitor =
        object (self : 'a)
          inherit [_] Abslayout0.mapreduce

          inherit [_] Util.list_monoid

          method add_agg aggs a =
            match
              List.find aggs ~f:(fun (_, a') -> [%compare.equal: pred] a a')
            with
            | Some (n, _) -> (Name n, aggs)
            | None ->
                let n = Fresh.name fresh "agg%d" |> Name.create in
                (Name n, (n, a) :: aggs)

          method! visit_Sum aggs p =
            let n, aggs' = self#add_agg aggs (Sum p) in
            (Sum n, aggs')

          method! visit_Count aggs =
            let n, aggs' = self#add_agg aggs Count in
            (Sum n, aggs')

          method! visit_Min aggs p =
            let n, aggs' = self#add_agg aggs (Min p) in
            (Min n, aggs')

          method! visit_Max aggs p =
            let n, aggs' = self#add_agg aggs (Max p) in
            (Max n, aggs')

          method! visit_Avg aggs p =
            let s, aggs' = self#add_agg aggs (Sum p) in
            let c, aggs'' = self#add_agg aggs' Count in
            (Binop (Div, Sum s, Sum c), aggs'')
        end
      in
      let outer_aggs, inner_aggs =
        List.fold_left outer_preds ~init:([], []) ~f:(fun (op, ip) p ->
            let p', ip' = visitor#visit_pred ip p in
            (op @ [p'], ip') )
      in
      let inner_aggs = List.map inner_aggs ~f:(fun (n, a) -> As_pred (a, n.name)) in
      (* Don't want to project out anything that we might need later. *)
      let inner_fields =
        Meta.(find_exn inner_rel schema) |> List.map ~f:(fun n -> Name n)
      in
      (outer_aggs, inner_aggs @ inner_fields)
    in
    let open A in
    { name= "push-select"
    ; f=
        (fun r ->
          let r = M.annotate_schema r in
          match r with
          | {node= Select (ps, {node= AHashIdx (r, r', m); _}); _} ->
              [hash_idx' r (select ps r') m]
          | {node= Select (ps, {node= AOrderedIdx (r, r', m); _}); _} ->
              let outer_aggs, inner_aggs = gen_concat_select_list ps r' in
              [select outer_aggs (ordered_idx r (select inner_aggs r') m)]
          | {node= Select (ps, {node= AList (r, r'); _}); _} ->
              let outer_aggs, inner_aggs = gen_concat_select_list ps r' in
              [select outer_aggs (list r (select inner_aggs r'))]
          | _ -> [] ) }
    |> run_everywhere

  (** Check that a predicate is applied to a schema that has the right fields. *)
  let predicate_is_valid p s =
    let visitor =
      object
        inherit [_] Abslayout0.reduce

        method zero = true

        method plus = ( && )

        method! visit_Exists () _ =
          (* TODO: Not correct. Account for parameters. *)
          true

        method! visit_First () _ =
          (* TODO: Not correct. Account for parameters. *)
          true

        method! visit_Name () n =
          let is_param = Option.is_none n.relation in
          let in_schema =
            List.mem s n ~equal:[%compare.equal: Name.Compare_no_type.t]
          in
          let is_valid = is_param || in_schema in
          if not is_valid then
            Logs.debug (fun m -> m "Predicate not valid. %a missing." Name.pp n) ;
          is_valid
      end
    in
    visitor#visit_pred () p

  let tf_hoist_filter _ =
    let open A in
    { name= "hoist-filter"
    ; f=
        (fun r ->
          let ret =
            match r.node with
            | OrderBy {key; rel= {node= Filter (p, r); _}; order} ->
                [(p, order_by key order r)]
            | GroupBy (ps, key, {node= Filter (p, r); _}) -> [(p, group_by ps key r)]
            | Filter (p, {node= Filter (p', r); _}) -> [(p', filter p r)]
            | Select (ps, {node= Filter (p, r); _}) -> [(p, select ps r)]
            | Join {pred; r1= {node= Filter (p, r); _}; r2} -> [(p, join pred r r2)]
            | Join {pred; r1; r2= {node= Filter (p, r); _}} -> [(p, join pred r1 r)]
            | _ -> []
          in
          List.filter_map ret ~f:(fun (p, r) ->
              let r = M.annotate_schema r in
              let schema = Meta.(find_exn r schema) in
              if predicate_is_valid p schema then Some (filter p r)
              else (
                Logs.debug (fun m ->
                    m "Cannot hoist: %a" Sexp.pp_hum
                      ([%sexp_of: Name.t list] schema) ) ;
                None ) ) ) }
    |> run_everywhere

  let tf_elim_join _ =
    let open A in
    { name= "elim-join"
    ; f=
        (function
        | {node= Join {pred; r1; r2}; _} -> [tuple [r1; filter pred r2] Cross]
        | _ -> []) }
    |> run_everywhere

  let tf_push_filter _ =
    let open A in
    { name= "push-filter"
    ; f=
        (function
        | {node= Filter (p, {node= Filter (p', r); _}); _} ->
            [filter p' (filter p r)]
        | {node= Filter (p, {node= Join {pred; r1; r2}; _}); _} ->
            [join (Binop (And, p, pred)) r1 r2]
        | {node= Filter (p, {node= AList (r, r'); _}); _} -> [list (filter p r) r']
        | {node= Filter (p, {node= AHashIdx (r, r', m); _}); _} ->
            [hash_idx' r (filter p r') m]
        | _ -> []) }
    |> run_everywhere

  let tf_merge_filter _ =
    let open A in
    { name= "merge-filter"
    ; f=
        (function
        | {node= Filter (p, {node= Filter (p', r); _}); _} ->
            [filter (Binop (And, p, p')) r]
        | _ -> []) }
    |> run_everywhere

  let tf_split_filter _ =
    let open A in
    { name= "split-filter"
    ; f=
        (function
        | {node= Filter (Binop (And, p, p'), r); _} -> [filter p (filter p' r)]
        | _ -> []) }
    |> run_everywhere

  let tf_project _ =
    let open A in
    { name= "project"
    ; f=
        (fun r ->
          let r = M.resolve ~params:Config.params r in
          let r = M.annotate_schema r in
          [project r] ) }

  let tf_hoist_join_pred _ =
    let open A in
    { name= "hoist-join-pred"
    ; f=
        (function
        | {node= Join {pred; r1; r2}; _} -> [filter pred (join (Bool true) r1 r2)]
        | _ -> []) }
    |> run_everywhere

  let eq_preds r =
    let visitor =
      object
        inherit [_] Abslayout0.reduce

        inherit [_] Util.list_monoid

        method! visit_Binop ps p =
          let op, arg1, arg2 = p in
          if [%compare.equal: A.binop] op Eq then (arg1, arg2) :: ps else ps
      end
    in
    visitor#visit_t [] r

  let replace_rel rel new_rel r =
    let visitor =
      object
        inherit [_] Abslayout0.endo

        method! visit_Scan () r' rel' =
          if String.(rel = rel') then new_rel.A.node else r'
      end
    in
    visitor#visit_t () r

  let tf_partition args =
    let open A in
    let name =
      match args with
      | [n] -> Name.of_string_exn n
      | _ -> failwith "Unexpected args."
    in
    let fresh_name =
      Caml.Format.sprintf "%s_%s" (Name.to_var name) (Fresh.name fresh "%d")
    in
    let rel = Name.rel_exn name in
    let filtered_rel =
      filter (Binop (Eq, Name name, Name (Name.create fresh_name))) (scan rel)
    in
    { name= "partition"
    ; f=
        (fun r ->
          [ list
              (dedup (select [As_pred (Name name, fresh_name)] (scan rel)))
              (replace_rel rel filtered_rel r) ] ) }
    |> run_everywhere ~stage:`Run

  let replace_pred r p1 p2 =
    let visitor =
      object
        inherit [_] Abslayout0.endo as super

        method! visit_pred () p =
          let p = super#visit_pred () p in
          if [%compare.equal: A.pred] p p1 then p2 else p
      end
    in
    visitor#visit_t () r

  let tf_partition_eq args =
    let open A in
    let name =
      match args with
      | [n] -> Name.of_string_exn n
      | _ -> failwith "Unexpected args."
    in
    let fresh_name =
      Caml.Format.sprintf "%s_%s" (Name.to_var name) (Fresh.name fresh "%d")
    in
    let rel = Name.rel_exn name in
    let filtered_rel =
      filter (Binop (Eq, Name name, Name (Name.create fresh_name))) (scan rel)
    in
    { name= "partition-eq"
    ; f=
        (fun r ->
          let eqs = eq_preds r in
          (* Any predicate that is compared for equality with the partition
              field is a candidate for the hash table key. *)
          let keys =
            List.filter_map eqs ~f:(fun (p1, p2) ->
                match (p1, p2) with
                | Name n, _ when String.(n.name = name.name) -> Some p2
                | _, Name n when String.(n.name = name.name) -> Some p1
                | _ -> None )
          in
          List.map keys ~f:(fun k ->
              (* The predicate that we chose as the key can be replaced by
                 `fresh_name`. *)
              hash_idx
                (dedup (select [As_pred (Name name, fresh_name)] (scan rel)))
                (replace_pred
                   (replace_rel rel filtered_rel r)
                   k
                   (Name (Name.create fresh_name)))
                [k] ) ) }
    |> run_everywhere ~stage:`Run

  let tf_split_out args =
    let open A in
    let rel, pk =
      match args with
      | [x; y] -> (x, Name.of_string_exn y)
      | _ ->
          Error.create "Unexpected args." args [%sexp_of: string list]
          |> Error.raise
    in
    let rel_schema = M.to_schema (scan rel) in
    let eq = [%compare.equal: Name.Compare_name_only.t] in
    { name= "split-out"
    ; f=
        (fun r ->
          let r = M.annotate_schema r in
          let schema = Meta.(find_exn r schema) in
          if List.mem schema pk ~equal:eq then
            let pk_fresh =
              sprintf "%s_%s" (Name.to_var pk) (Fresh.name fresh "%d")
            in
            let sel_list =
              List.filter schema ~f:(fun n ->
                  eq n pk || not (List.mem rel_schema n ~equal:eq) )
              |> List.map ~f:(fun n -> Name n)
            in
            [ tuple
                [ select sel_list r
                ; hash_idx
                    (dedup
                       (select
                          [As_pred (Name pk, pk_fresh)]
                          (as_ (Option.value_exn pk.relation) (scan rel))))
                    (filter
                       (Binop (Eq, Name pk, Name (Name.create pk_fresh)))
                       (as_ (Option.value_exn pk.relation) (scan rel)))
                    [Name pk] ]
                Cross ]
          else [] ) }
    |> run_everywhere ~stage:`Run

  let tf_partition_size args =
    let open A in
    let field =
      match args with
      | [x] -> Name.of_string_exn x
      | _ -> failwith "Unexpected args."
    in
    { name= "partition-size"
    ; f=
        (fun r ->
          [ tuple
              (List.map
                 [(2 ** 7) - 1; (2 ** 15) - 1; (2 ** 23) - 1; (2 ** 31) - 1]
                 ~f:(fun max ->
                   filter
                     (Binop
                        ( And
                        , Binop (Lt, Name field, Int max)
                        , Binop (Gt, Name field, Int (-max)) ))
                     r ))
              Concat ] ) }
    |> run_everywhere ~stage:`Run

  let tf_hoist_pred_constant _ =
    let open A in
    let f r =
      let r = M.annotate_schema r in
      annotate_free r ;
      let consts =
        match r.node with
        | Filter (p, r') -> pred_constants Meta.(find_exn r' schema) p
        | Select (ps, r') ->
            List.concat_map ps ~f:(pred_constants Meta.(find_exn r' schema))
            |> List.dedup_and_sort ~compare:[%compare: pred]
        | _ -> []
      in
      let fresh_id = Fresh.name fresh "const%d" in
      let fresh_name = Name.create fresh_id in
      List.map consts ~f:(fun p ->
          tuple
            [scalar (As_pred (p, fresh_id)); subst_single p (Name fresh_name) r]
            Cross )
    in
    {name= "hoist-pred-const"; f} |> run_everywhere ~stage:`Run

  let transforms =
    [ ("hoist-join-pred", tf_hoist_join_pred)
    ; ("elim-groupby", tf_elim_groupby)
    ; ("elim-groupby-filter", tf_elim_groupby_filter)
    ; ("push-orderby", tf_push_orderby)
    ; ("hoist-filter", tf_hoist_filter)
    ; ("push-filter", tf_push_filter)
    ; ("merge-filter", tf_merge_filter)
    ; ("split-filter", tf_split_filter)
    ; ("elim-eq-filter", tf_elim_eq_filter)
    ; ("elim-cmp-filter", tf_elim_cmp_filter)
    ; ("elim-join", tf_elim_join)
    ; ("row-store", tf_row_store)
    ; ("project", tf_project)
    ; ("push-select", tf_push_select)
    ; ("partition", tf_partition)
    ; ("partition-eq", tf_partition_eq)
    ; ("partition-size", tf_partition_size)
    ; ("split-out", tf_split_out)
    ; ("hoist-pred-const", tf_hoist_pred_constant) ]

  let of_string_exn s =
    let tf_strs =
      try
        Transform_parser.transforms_eof Transform_lexer.token (Lexing.from_string s)
      with Parser_utils.ParseError (msg, line, col) as e ->
        Logs.err (fun m -> m "Parse error: %s (line: %d, col: %d)" msg line col) ;
        raise e
    in
    List.map tf_strs ~f:(fun (name, args, index) ->
        let _, tf_gen =
          List.find_exn transforms ~f:(fun (n, _) -> String.(name = n))
        in
        let tf = tf_gen args in
        match index with
        | Some i -> {tf with f= (fun r -> [List.nth_exn (run tf r) i])}
        | None -> {tf with f= (fun r -> run tf r)} )
end
