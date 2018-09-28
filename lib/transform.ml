open Base
open Printf
open Collections
module A = Abslayout

module Config = struct
  module type S = sig
    include Eval.Config.S

    val check_transforms : bool

    val params : Set.M(Name.Compare_no_type).t
  end
end

module Make (Config : Config.S) (M : Abslayout_db.S) () = struct
  type t = {name: string; f: A.t -> A.t list} [@@deriving sexp]

  let fresh = Fresh.create ()

  let run_everywhere {name; f= f_inner} =
    let rec f r =
      let rs = f_inner r in
      let rs' =
        match r.node with
        | A.Scan _ | AEmpty | AScalar _ -> []
        | Dedup r -> List.map ~f:A.dedup (f r)
        | As (n, r) -> List.map ~f:(A.as_ n) (f r)
        | OrderBy {key; order; rel} -> List.map (f rel) ~f:(A.order_by key order)
        | AList (r1, r2) ->
            List.map (f r1) ~f:(fun r1 -> A.list r1 r2)
            @ List.map (f r2) ~f:(fun r2 -> A.list r1 r2)
        | Select (fs, r') -> List.map (f r') ~f:(A.select fs)
        | Filter (ps, r') -> List.map (f r') ~f:(A.filter ps)
        | Join {r1; r2; pred} ->
            List.map (f r1) ~f:(fun r1 -> A.join pred r1 r2)
            @ List.map (f r2) ~f:(fun r2 -> A.join pred r1 r2)
        | GroupBy (x, y, r') -> List.map (f r') ~f:(A.group_by x y)
        | AHashIdx (r1, r2, m) ->
            List.map (f r1) ~f:(fun r1 -> A.hash_idx r1 r2 m)
            @ List.map (f r2) ~f:(fun r2 -> A.hash_idx r1 r2 m)
        | AOrderedIdx (r1, r2, m) ->
            List.map (f r1) ~f:(fun r1 -> A.ordered_idx r1 r2 m)
            @ List.map (f r2) ~f:(fun r2 -> A.ordered_idx r1 r2 m)
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
    let rs = t.f r in
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
        r' )

  let run = if Config.check_transforms then run_checked else run_unchecked

  let id = {name= "id"; f= (fun r -> [r])}

  let compose {name= n1; f= f1} {name= n2; f= f2} =
    { name= sprintf "%s,%s" n2 n1
    ; f= (fun r -> List.concat_map ~f:(fun x -> f1 x) (f2 r)) }

  let compose_many = List.fold_left ~init:id ~f:compose

  let no_params r = Set.is_empty (Set.inter (A.names r) Config.params)

  let tf_row_store =
    let open A in
    { name= "row-store"
    ; f=
        (fun r ->
          if no_params r then
            let s = M.to_schema r in
            let scalars = List.map s ~f:(fun n -> scalar (Name n)) in
            [list r (tuple scalars Cross)]
          else [] ) }
    |> run_everywhere

  let tf_elim_groupby =
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
            [ list
                (as_ key_name (dedup (select key_preds r)))
                (select ps (filter filter_pred r)) ]
        | _ -> []) }
    |> run_everywhere

  let tf_elim_groupby_filter =
    let open A in
    { name= "elim-groupby-filter"
    ; f=
        (function
        | {node= GroupBy (ps, key, {node= Filter (p, r); _}); _} when no_params r ->
            let key_name = Fresh.name fresh "k%d" in
            let new_key =
              List.map key ~f:(fun n -> sprintf "%s_%s" key_name (Name.to_var n))
            in
            let select_list =
              List.map2_exn key new_key ~f:(fun n n' -> As_pred (Name n, n'))
            in
            let filter_pred =
              List.map2_exn key new_key ~f:(fun n n' ->
                  Binop (Eq, Name n, Name (Name.create n')) )
              |> List.fold_left1 ~f:(fun acc p -> Binop (And, acc, p))
            in
            [ list
                (dedup (select select_list r))
                (select ps (filter p (filter filter_pred r))) ]
        | _ -> []) }
    |> run_everywhere

  let tf_push_orderby =
    let open A in
    let same_orders r1 r2 =
      let r1 = M.annotate_schema r1 in
      let r2 = M.annotate_schema r2 in
      annotate_eq r1 ;
      annotate_orders r1 ;
      annotate_eq r2 ;
      annotate_orders r2 ;
      [%compare.equal: pred list] Meta.(find_exn r1 order) Meta.(find_exn r2 order)
    in
    let orderby_list key order r1 r2 =
      let r1 = M.annotate_schema r1 in
      let r2 = M.annotate_schema r2 in
      annotate_eq r1 ;
      annotate_eq r2 ;
      let schema1 = Meta.(find_exn r1 schema) in
      let open Core in
      let eq_map =
        Meta.(find_exn r2 eq)
        |> List.fold_left
             ~init:(Map.empty (module Name.Compare_no_type))
             ~f:(fun m (n, n') ->
               let s = Union_find.create n in
               let s' = Union_find.create n' in
               Union_find.union s s' ;
               let m = Map.add_exn m ~key:n ~data:s in
               let m = Map.add_exn m ~key:n' ~data:s' in
               m )
      in
      let key_in_schema1 =
        List.for_all key ~f:(function
          | Name n ->
              let s =
                match Map.find eq_map n with
                | Some s -> s
                | None -> Union_find.create n
              in
              List.exists schema1 ~f:(fun n' ->
                  let s' =
                    match Map.find eq_map n' with
                    | Some s -> s
                    | None -> Union_find.create n'
                  in
                  Union_find.same_class s s' )
          | _ -> false )
      in
      if key_in_schema1 then
        let new_key =
          List.map key ~f:(function
            | Name n ->
                let s =
                  match Map.find eq_map n with
                  | Some s -> s
                  | None -> Union_find.create n
                in
                let n' =
                  List.find_exn schema1 ~f:(fun n' ->
                      let s' =
                        match Map.find eq_map n' with
                        | Some s -> s
                        | None -> Union_find.create n'
                      in
                      Union_find.same_class s s' )
                in
                Name n'
            | _ -> failwith "" )
        in
        [list (order_by new_key order r1) r2]
      else []
    in
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
            | _ -> []
          in
          List.filter rs ~f:(same_orders r) ) }
    |> run_everywhere

  let tf_hoist_filter =
    let open A in
    { name= "hoist-filter"
    ; f=
        (function
        | {node= OrderBy {key; rel= {node= Filter (p, r); _}; order}; _} ->
            [filter p (order_by key order r)]
        | {node= GroupBy (ps, key, {node= Filter (p, r); _}); _} ->
            [filter p (group_by ps key r)]
        | {node= Filter (p, {node= Filter (p', r); _}); _} ->
            [filter p' (filter p r)]
        | {node= Select (ps, {node= Filter (p, r); _}); _} ->
            [filter p (select ps r)]
        | {node= Join {pred; r1= {node= Filter (p, r); _}; r2}; _} ->
            [filter p (join pred r r2)]
        | {node= Join {pred; r1; r2= {node= Filter (p, r); _}}; _} ->
            [filter p (join pred r1 r)]
        | _ -> []) }
    |> run_everywhere

  let tf_push_filter =
    let open A in
    { name= "push-filter"
    ; f=
        (function
        | {node= Filter (p, {node= Filter (p', r); _}); _} ->
            [filter p' (filter p r)]
        | {node= Filter (p, {node= AList (r, r'); _}); _} -> [list (filter p r) r']
        | _ -> []) }
    |> run_everywhere

  let tf_project =
    let open A in
    { name= "project"
    ; f=
        (fun r ->
          let r = M.annotate_schema r in
          annotate_needed r ; [project r] ) }

  let transforms =
    [ tf_elim_groupby
    ; tf_elim_groupby_filter
    ; tf_push_orderby
    ; tf_hoist_filter
    ; tf_push_filter
    ; tf_row_store
    ; tf_project ]

  let of_name : string -> t Or_error.t =
   fun n ->
    let m_tf = List.find transforms ~f:(fun {name; _} -> String.(name = n)) in
    match m_tf with
    | Some x -> Ok x
    | None -> Or_error.error "Transform not found." n [%sexp_of: string]

  let of_name_exn : string -> t = fun n -> Or_error.ok_exn (of_name n)
end
