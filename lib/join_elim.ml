open Core
open Ast
module A = Abslayout
module V = Visitors
include (val Log.make "castor.join-elim")

(** Given two sets of names and an equivalence relation, return a translation
   from one set to the other. *)
let translate eqs ~from ~to_ =
  let open Option.Let_syntax in
  let eqs = Set.to_list eqs in
  debug (fun m -> m "Eqs: %a" Fmt.Dump.(list @@ pair Name.pp Name.pp) eqs);
  debug (fun m -> m "From: %a" Fmt.Dump.(list @@ Name.pp) from);
  debug (fun m -> m "To: %a" Fmt.Dump.(list @@ Name.pp) to_);

  let classes =
    from @ to_ @ List.concat_map eqs ~f:(fun (n, n') -> [ n; n' ])
    |> List.dedup_and_sort ~compare:[%compare: Name.t]
    |> List.map ~f:(fun n -> (n, Union_find.create n))
    |> Map.of_alist_exn (module Name)
  in
  List.iter eqs ~f:(fun (n, n') ->
      Union_find.union (Map.find_exn classes n) (Map.find_exn classes n'));

  List.map from ~f:(fun n ->
      let c = Map.find_exn classes n in
      let n' =
        List.find to_ ~f:(fun n' ->
            let c' = Map.find_exn classes n' in
            Union_find.same_class c c')
      in
      if Option.is_none n' then
        debug (fun m -> m "Failed to find replacement for %a." Name.pp n);
      let%map n' = n' in
      (n, n'))
  |> Option.all

(** Compute an extension of the rhs that covers the attributes from the lhs. *)
let extension eqs schema_lhs schema_rhs =
  let open Option.Let_syntax in
  let%map ext = translate eqs ~from:schema_lhs ~to_:schema_rhs in
  List.map ext ~f:(fun (n, n') -> (Name n', Name.name n))

let try_extend eqs pred lhs rhs =
  let schema_lhs =
    Schema.schema_opt lhs
    |> List.filter_map ~f:(fun (n, _) -> Option.map n ~f:Name.create)
  and schema_rhs =
    Schema.schema_opt rhs
    |> List.filter_map ~f:(fun (n, _) -> Option.map n ~f:Name.create)
  in
  match extension eqs schema_lhs schema_rhs with
  | Some ex ->
      let sl = ex @ Schema.to_select_list schema_rhs in
      debug (fun m ->
          m "Extending with:@ %a@ to avoid join of:@ %a@ with:@ %a"
            (Fmt.Dump.list @@ Fmt.Dump.pair Pred.pp Fmt.string)
            sl A.pp lhs A.pp rhs);
      Select (sl, rhs)
  | None ->
      debug (fun m ->
          m "Failed to remove join of:@ %a@ with:@ %a" A.pp lhs A.pp rhs);
      Join { pred; r1 = lhs; r2 = rhs }

let remove_joins r =
  let rec annot r = V.Map.annot (query r.meta) r
  and query meta = function
    | Join { r1; r2; pred = p } when not meta#meta#cardinality_matters ->
        let r1 = annot r1 and r2 = annot r2 in
        try_extend r1.meta#eqs p r1 r2
    | Join { pred = p; _ } as q ->
        debug (fun m ->
            m "Join cardinality matters for@ %a@ %s" Pred.pp p
              meta#meta#why_card_matters);
        V.Map.query annot pred q
    | q -> V.Map.query annot pred q
  and pred p = V.Map.pred annot pred p in
  Equiv.Context.annotate r |> annot

let remove_dedup r =
  let rec annot r = V.Map.annot (query r.meta) r
  and query meta = function
    | Dedup r' when not meta#cardinality_matters -> (annot r').node
    | Dedup r' ->
        debug (fun m -> m "Dedup cardinality matters: %s" meta#why_card_matters);
        Dedup (annot r')
    | q -> V.Map.query annot pred q
  and pred p = V.Map.pred annot pred p in
  annot r
