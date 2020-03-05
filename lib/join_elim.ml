module A = Abslayout
open Abslayout_visitors

let src = Logs.Src.create "castor.join_elim"

module Log = (val Logs.src_log src : Logs.LOG)

let () = Logs.Src.set_level src (Some Info)

(** Compute an extension of the rhs that covers the attributes from the lhs. *)
let extension eqs lhs rhs =
  let schema_lhs = Schema.schema lhs
  and schema_rhs = Schema.schema rhs
  and eqs = Set.to_list eqs in

  let classes =
    schema_lhs @ schema_rhs @ List.concat_map eqs ~f:(fun (n, n') -> [ n; n' ])
    |> List.dedup_and_sort ~compare:[%compare: Name.t]
    |> List.map ~f:(fun n -> (n, Union_find.create n))
    |> Map.of_alist_exn (module Name)
  in
  List.iter eqs ~f:(fun (n, n') ->
      Union_find.union (Map.find_exn classes n) (Map.find_exn classes n'));

  List.map schema_lhs ~f:(fun n ->
      let c = Map.find_exn classes n in
      let n' =
        List.find schema_rhs ~f:(fun n' ->
            let c' = Map.find_exn classes n' in
            Union_find.same_class c c')
      in
      if Option.is_none n' then
        Log.debug (fun m -> m "Failed to find replacement for %a." Name.pp n);
      Option.map n' ~f:(fun n' -> As_pred (Name n', Name.name n)))
  |> Option.all

let try_extend eqs pred lhs rhs =
  match extension eqs lhs rhs with
  | Some ex ->
      let sl = ex @ Schema.to_select_list @@ Schema.schema rhs in
      Log.debug (fun m ->
          m "Extending with:@ %a@ to avoid join of:@ %a@ with:@ %a"
            (Fmt.Dump.list Pred.pp) sl A.pp lhs A.pp rhs);
      Select (sl, rhs)
  | None -> Join { pred; r1 = lhs; r2 = rhs }

let remove_joins r =
  let rec annot r = map_annot (query r.meta) r
  and query meta = function
    | Join { r1; r2; pred = p } when not meta#meta#cardinality_matters ->
        let r1 = annot r1 and r2 = annot r2 in
        try_extend r1.meta#eqs p r1 r2
    | q -> map_query annot pred q
  and pred p = map_pred annot pred p in
  Equiv.Context.annotate r |> annot
